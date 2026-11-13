// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"fmt"
	"io"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/scritchley/orc"
	orcpb "github.com/scritchley/orc/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// Reject stripe features the legacy decoder cannot interpret correctly:
// date presence streams and non-UTC timestamp writer timezones.
func validateORCStripeStreams(file iceio.File, fileSize int64, codec orcpb.CompressionKind, footer *orcpb.Footer) error {
	dateIDs := make(map[uint32]struct{})
	hasTimestamp := false
	for id, typ := range footer.GetTypes() {
		switch typ.GetKind() {
		case orcpb.Type_DATE:
			dateIDs[uint32(id)] = struct{}{}
		case orcpb.Type_TIMESTAMP:
			hasTimestamp = true
		}
	}
	if len(dateIDs) == 0 && !hasTimestamp {
		return nil
	}
	for _, stripe := range footer.GetStripes() {
		// Check each addition before converting the offsets to int64.
		limit := uint64(fileSize)
		offset, index, data, length := stripe.GetOffset(), stripe.GetIndexLength(), stripe.GetDataLength(), stripe.GetFooterLength()
		if offset > limit || index > limit-offset || data > limit-offset-index || length > limit-offset-index-data || length > uint64(maxORCFooterBytes) {
			return fmt.Errorf("%w: invalid ORC stripe footer range", iceberg.ErrInvalidSchema)
		}
		source := io.NewSectionReader(file, int64(offset+index+data), int64(length))
		var decoded io.Reader
		switch codec {
		case orcpb.CompressionKind_NONE:
			decoded = source
		case orcpb.CompressionKind_ZLIB:
			decoded = orc.CompressionZlib{}.Decoder(source)
		case orcpb.CompressionKind_SNAPPY:
			decoded = orc.CompressionSnappy{}.Decoder(source)
		default:
			return fmt.Errorf("%w: ORC compression %s", iceberg.ErrNotImplemented, codec)
		}
		payload, err := io.ReadAll(io.LimitReader(decoded, maxORCFooterBytes+1))
		if err != nil {
			return fmt.Errorf("read ORC stripe footer: %w", err)
		}
		if int64(len(payload)) > maxORCFooterBytes {
			return fmt.Errorf("%w: ORC stripe footer exceeds size limit", iceberg.ErrInvalidSchema)
		}
		var stripeFooter orcpb.StripeFooter
		if err := proto.Unmarshal(payload, protoadapt.MessageV2Of(&stripeFooter)); err != nil {
			return fmt.Errorf("decode ORC stripe footer: %w", err)
		}
		if hasTimestamp {
			// The decoder assumes a UTC base and never applies the writer's
			// DST or historical offset changes. Missing zones retain its
			// legacy UTC interpretation; only fixed UTC aliases are supported.
			switch zone := stripeFooter.GetWriterTimezone(); zone {
			case "", "UTC", "GMT", "Etc/UTC", "Etc/GMT":
			default:
				return fmt.Errorf("%w: ORC timestamp writer timezone %q", iceberg.ErrNotImplemented, zone)
			}
		}
		for _, stream := range stripeFooter.GetStreams() {
			if stream.GetKind() == orcpb.Stream_PRESENT {
				if _, ok := dateIDs[stream.GetColumn()]; ok {
					// The decoder loses nulls for date columns with a presence stream.
					return fmt.Errorf("%w: ORC dates with a presence stream", iceberg.ErrNotImplemented)
				}
			}
		}
	}

	return nil
}
