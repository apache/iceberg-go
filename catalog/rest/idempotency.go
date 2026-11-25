// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package rest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

const (
	keyedMaxRetries = 3
	keyedMinDelay   = 100 * time.Millisecond
	keyedMaxDelay   = 5 * time.Second
)

// errReplayWindowClosed is returned by a keyed request's GetBody once the key
// lifetime no longer allows a resend, which stops net/http from replaying it.
var errReplayWindowClosed = errors.New("rest: idempotency-key-lifetime does not allow a resend")

// idempotencyPolicy marks a request as keyed: it carries an Idempotency-Key and
// may be resent within the server's advertised key lifetime.
type idempotencyPolicy struct {
	lifetime *string // advertised idempotency-key-lifetime, nil if not advertised
	key      *string // caller-supplied key (scan planning); nil generates one
	onRetry  func()  // called before each explicit resend
}

// withIdempotency keys a request when the server advertised a lifetime or the
// caller supplied a key.
func withIdempotency(lifetime, key *string, onRetry func()) reqOption {
	return func(c *reqConfig) {
		if lifetime != nil || key != nil {
			c.idempotency = &idempotencyPolicy{lifetime: lifetime, key: key, onRetry: onRetry}
		}
	}
}

// withCommitState makes a keyed call that cannot be resent after an ambiguous
// failure report ErrCommitStateUnknown, since the commit may have landed.
func withCommitState() reqOption {
	return func(c *reqConfig) { c.commit = true }
}

// keyedCall is one logical keyed call: its key and the window in which it may
// be resent. A new call, including a transaction retry after a 409, gets a new
// key.
type keyedCall struct {
	policy   *idempotencyPolicy
	commit   bool
	key      string
	start    time.Time     // first attempt, or the caller key's creation if earlier
	lifetime time.Duration // 0 when resends are not allowed
	now      func() time.Time
}

func newKeyedCall(cfg *reqConfig) (*keyedCall, error) {
	if cfg.idempotency == nil {
		return nil, nil
	}
	key, err := idempotencyHeaderValue(cfg.idempotency.key)
	if err != nil {
		return nil, err
	}
	kc := &keyedCall{policy: cfg.idempotency, commit: cfg.commit, key: key, start: time.Now(), now: time.Now}
	if cfg.idempotency.lifetime != nil {
		kc.lifetime, _ = parseKeyLifetime(*cfg.idempotency.lifetime)
	}
	if cfg.idempotency.key != nil {
		// A caller key's age counts against the window. The UUIDv7 timestamp may
		// predate the caller's first send, which only shortens the window; a
		// future timestamp is clamped so it never lengthens it.
		id, err := uuid.Parse(key)
		if err != nil {
			return nil, err
		}
		ms := int64(id[0])<<40 | int64(id[1])<<32 | int64(id[2])<<24 | int64(id[3])<<16 | int64(id[4])<<8 | int64(id[5])
		if created := time.UnixMilli(ms); created.Before(kc.start) {
			kc.start = created
		}
	}

	return kc, nil
}

func (kc *keyedCall) open() bool { return kc.now().Sub(kc.start) < kc.lifetime }

// observedBody distinguishes a signer's body read from a transport replay.
type observedBody struct {
	io.ReadCloser
	sent *atomic.Bool
}

func (b observedBody) Read(p []byte) (int, error) {
	b.sent.Store(true)

	return b.ReadCloser.Read(p)
}

func (b observedBody) Close() error {
	b.sent.Store(true)

	return b.ReadCloser.Close()
}

// prepare adds the key and bounds transport replays and redirects with bodies.
// Bodyless requests (keyed DELETE) never call GetBody and cannot be bounded here.
func (kc *keyedCall) prepare(req *http.Request) {
	req.Header.Set(headerIdempotencyKey, kc.key)
	if body := req.GetBody; body != nil {
		sent := &atomic.Bool{}
		req.Body = observedBody{ReadCloser: req.Body, sent: sent}
		req.GetBody = func() (io.ReadCloser, error) {
			if sent.Load() && !kc.open() {
				return nil, errReplayWindowClosed
			}

			return body()
		}
	}
}

func keyedRetryStatus(code int) bool {
	switch code {
	case http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}

	return false
}

// keyedRetryError reports whether a transport error may be resent: a timeout
// or connection error, but not the caller's own cancellation.
func keyedRetryError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if errors.Is(err, errReplayWindowClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if ue, ok := errors.AsType[*url.Error](err); ok {
		err = ue.Err
	}
	_, ok := errors.AsType[net.Error](err)

	return ok
}

// send performs one HTTP call. An unkeyed call (kc == nil) is a single cl.Do.
// A keyed call resends the same request and key after a retryable 5xx, timeout or connection
// error while the key lifetime allows it, and returns every other response as
// is. It returns either a response to handle or the final error; a retryable
// status that was not resent is returned as the error handleNon200 maps it to.
func send(ctx context.Context, cl *http.Client, kc *keyedCall, cfg *reqConfig, override map[int]error, newReq func() (*http.Request, error)) (response *http.Response, finalErr error) {
	resent := false
	defer func() {
		if !resent || !kc.commit || finalErr == nil || errors.Is(finalErr, ErrCommitStateUnknown) {
			return
		}
		// After a resend, a conflict or rejection may answer a commit that already
		// landed; keep its text but not its chain, or the table commit loop would
		// replay it (conflict) or clean up its files (rejection).
		if errors.Is(finalErr, table.ErrCommitFailed) || errors.Is(finalErr, table.ErrCommitRejected) {
			finalErr = fmt.Errorf("%w: resend failed: %v", ErrCommitStateUnknown, finalErr)
		} else {
			finalErr = fmt.Errorf("%w: resend failed: %w", ErrCommitStateUnknown, finalErr)
		}
	}()
	_, hasDeadline := ctx.Deadline()
	delay := keyedMinDelay
	for attempt := 0; ; attempt++ {
		req, err := newReq()
		if err != nil {
			return nil, err
		}
		if kc == nil {
			return cl.Do(req)
		}
		kc.prepare(req)
		if err := ctx.Err(); err != nil {
			if attempt > 0 && kc.commit {
				return nil, fmt.Errorf("%w: cancelled after an ambiguous attempt: %w", ErrCommitStateUnknown, err)
			}

			return nil, err
		}
		resent = attempt > 0
		rsp, err := cl.Do(req)
		var retryAfter time.Duration
		switch {
		case err == nil && resent && kc.commit && rsp.StatusCode >= 400 && !keyedRetryStatus(rsp.StatusCode):
			err = handleNon200(rsp, override, cfg.errorTypeOverride)
			_, _ = io.Copy(io.Discard, rsp.Body)
			_ = rsp.Body.Close()

			return nil, err
		case err == nil && !keyedRetryStatus(rsp.StatusCode):
			return rsp, nil
		case err == nil:
			retryAfter = parseRetryAfter(rsp.Header.Get("Retry-After"))
			err = handleNon200(rsp, override, cfg.errorTypeOverride)
			_, _ = io.Copy(io.Discard, rsp.Body)
			_ = rsp.Body.Close()
		case !keyedRetryError(ctx, err):
			if kc.commit && ctx.Err() != nil {
				return nil, fmt.Errorf("%w: cancelled after an ambiguous attempt: %w: %w", ErrCommitStateUnknown, err, ctx.Err())
			}

			return nil, err
		}

		delay = nextScanPlanBackoff(delay, keyedMinDelay, keyedMaxDelay)
		wait := applyRetryAfter(delay, retryAfter, keyedMinDelay, keyedMaxDelay, hasDeadline)
		if attempt >= keyedMaxRetries || kc.lifetime <= 0 || kc.now().Sub(kc.start)+wait >= kc.lifetime {
			if kc.commit && !errors.Is(err, ErrCommitStateUnknown) {
				err = fmt.Errorf("%w: not resent within idempotency-key-lifetime: %w", ErrCommitStateUnknown, err)
			}

			return nil, err
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			if kc.commit {
				return nil, fmt.Errorf("%w: cancelled after an ambiguous attempt: %w", ErrCommitStateUnknown, ctx.Err())
			}

			return nil, ctx.Err()
		case <-timer.C:
			if !kc.open() {
				if kc.commit && !errors.Is(err, ErrCommitStateUnknown) {
					err = fmt.Errorf("%w: not resent within idempotency-key-lifetime: %w", ErrCommitStateUnknown, err)
				}

				return nil, err
			}
			if kc.policy.onRetry != nil {
				kc.policy.onRetry()
			}
		}
	}
}

// keyLifetimeRE matches Java Duration.parse's day/time syntax.
var keyLifetimeRE = regexp.MustCompile(`(?i)^([+-]?)P(?:([+-]?\d+)D)?(?:T(?:([+-]?\d+)H)?(?:([+-]?\d+)M)?(?:([+-]?\d+(?:[.,]\d{0,9})?)S)?)?$`)

// parseKeyLifetime accepts positive durations; zero, negative and malformed
// values disable resends. Java allows unbounded resends for malformed values.
func parseKeyLifetime(s string) (time.Duration, bool) {
	for i := range len(s) {
		if s[i] >= 0x80 {
			return 0, false
		}
	}
	m := keyLifetimeRE.FindStringSubmatch(s)
	if m == nil || strings.EqualFold(s[len(s)-1:], "T") {
		return 0, false
	}
	total := new(big.Rat)
	var wholeSeconds [4]*big.Int
	for i, unit := range []int64{86400, 3600, 60, 1} {
		if m[i+2] == "" {
			continue
		}
		component := m[i+2]
		integer := component
		if dot := strings.IndexAny(integer, ".,"); dot >= 0 {
			integer = integer[:dot]
		}
		whole, ok := new(big.Int).SetString(integer, 10)
		if !ok || !whole.IsInt64() {
			return 0, false
		}
		whole.Mul(whole, big.NewInt(unit))
		if !whole.IsInt64() {
			return 0, false
		}
		wholeSeconds[i] = whole
		value, ok := new(big.Rat).SetString(strings.ReplaceAll(component, ",", "."))
		if !ok {
			return 0, false
		}
		total.Add(total, value.Mul(value, big.NewRat(unit, 1)))
	}
	// Duration.parse adds seconds from smallest unit to largest with overflow checks.
	sum := new(big.Int)
	for i := len(wholeSeconds) - 1; i >= 0; i-- {
		if wholeSeconds[i] != nil {
			sum.Add(sum, wholeSeconds[i])
			if !sum.IsInt64() {
				return 0, false
			}
		}
	}
	minSeconds := new(big.Rat).SetInt64(math.MinInt64)
	maxSeconds := new(big.Rat).Neg(minSeconds) // exclusive upper bound
	if total.Cmp(minSeconds) < 0 || total.Cmp(maxSeconds) >= 0 {
		return 0, false
	}
	if m[1] == "-" {
		total.Neg(total)
	}
	if total.Sign() <= 0 || total.Cmp(maxSeconds) >= 0 {
		return 0, false
	}
	total.Mul(total, big.NewRat(1e9, 1))
	nanos := new(big.Int).Quo(total.Num(), total.Denom())
	if !nanos.IsInt64() {
		return math.MaxInt64, true
	}

	return time.Duration(nanos.Int64()), true
}
