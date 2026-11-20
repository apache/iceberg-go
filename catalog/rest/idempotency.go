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
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"

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
}

func newKeyedCall(cfg *reqConfig) (*keyedCall, error) {
	if cfg.idempotency == nil {
		return nil, nil
	}
	key, err := idempotencyHeaderValue(cfg.idempotency.key)
	if err != nil {
		return nil, err
	}
	kc := &keyedCall{policy: cfg.idempotency, commit: cfg.commit, key: key, start: time.Now()}
	if cfg.idempotency.lifetime != nil {
		kc.lifetime, _ = parseKeyLifetime(*cfg.idempotency.lifetime)
	}
	if cfg.idempotency.key != nil {
		// A caller key's age counts against the window. The UUIDv7 timestamp may
		// predate the caller's first send, which only shortens the window; a
		// future timestamp is clamped so it never lengthens it.
		id := uuid.MustParse(key)
		ms := int64(id[0])<<40 | int64(id[1])<<32 | int64(id[2])<<24 | int64(id[3])<<16 | int64(id[4])<<8 | int64(id[5])
		if created := time.UnixMilli(ms); created.Before(kc.start) {
			kc.start = created
		}
	}

	return kc, nil
}

func (kc *keyedCall) open() bool { return time.Since(kc.start) < kc.lifetime }

// prepare adds the key and stops net/http replaying the request on its own
// (reused-connection failures, 307/308 redirects) once the window is closed.
// Bodyless requests (keyed DELETE) never call GetBody, so their transport
// replay cannot be bounded here.
func (kc *keyedCall) prepare(req *http.Request) {
	req.Header.Set(headerIdempotencyKey, kc.key)
	if body := req.GetBody; body != nil {
		req.GetBody = func() (io.ReadCloser, error) {
			if !kc.open() {
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

// send performs one HTTP call; kc is nil for an unkeyed call. An unkeyed call is a single cl.Do. A keyed call
// resends the same request and key after a retryable 5xx, timeout or connection
// error while the key lifetime allows it, and returns every other response as
// is. It returns either a response to handle or the final error; a retryable
// status that was not resent is returned as the error handleNon200 maps it to.
func send(ctx context.Context, cl *http.Client, kc *keyedCall, cfg *reqConfig, override map[int]error, newReq func() (*http.Request, error)) (*http.Response, error) {
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
		rsp, err := cl.Do(req)
		var retryAfter time.Duration
		switch {
		case err == nil && !keyedRetryStatus(rsp.StatusCode):
			return rsp, nil
		case err == nil:
			retryAfter = parseRetryAfter(rsp.Header.Get("Retry-After"))
			err = handleNon200(rsp, override, cfg.errorTypeOverride)
			_, _ = io.Copy(io.Discard, rsp.Body)
			_ = rsp.Body.Close()
		case !keyedRetryError(ctx, err):
			return nil, err
		}

		delay = nextScanPlanBackoff(delay, keyedMinDelay, keyedMaxDelay)
		wait := applyRetryAfter(delay, retryAfter, keyedMinDelay, keyedMaxDelay, hasDeadline)
		if attempt >= keyedMaxRetries || kc.lifetime <= 0 || time.Since(kc.start)+wait >= kc.lifetime {
			if kc.commit && !errors.Is(err, ErrCommitStateUnknown) {
				err = fmt.Errorf("%w: not resent within idempotency-key-lifetime: %w", ErrCommitStateUnknown, err)
			}

			return nil, err
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()

			return nil, ctx.Err()
		case <-timer.C:
			if kc.policy.onRetry != nil {
				kc.policy.onRetry()
			}
		}
	}
}

// keyLifetimeRE matches the ISO-8601 duration form Java's Duration.parse
// accepts for a non-negative duration: PnDTnHnMn.nS.
var keyLifetimeRE = regexp.MustCompile(`^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$`)

// parseKeyLifetime returns the advertised lifetime and whether it is usable:
// it must parse and be positive. Unlike Java, which stops bounding resends on a
// malformed value, Go does not resend keyed requests then.
func parseKeyLifetime(s string) (time.Duration, bool) {
	m := keyLifetimeRE.FindStringSubmatch(s)
	if m == nil || s[len(s)-1] == 'T' {
		return 0, false
	}
	var secs float64
	for i, unit := range []float64{86400, 3600, 60, 1} {
		if m[i+1] == "" {
			continue
		}
		v, err := strconv.ParseFloat(m[i+1], 64)
		if err != nil {
			return 0, false
		}
		secs += v * unit
	}
	if secs <= 0 {
		return 0, false
	}
	if secs >= math.MaxInt64/1e9 {
		return math.MaxInt64, true
	}

	return time.Duration(secs * 1e9), true
}
