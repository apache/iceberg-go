// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iceberg

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"math"

	"github.com/twmb/avro/ocf"
)

// manifestDeflateCodec reuses the inflater and output buffer across blocks of
// one manifest. The avro decoder copies strings and bytes out of the block unless
// avro.AliasInput is set, and ocf.Reader drops that option, so the output buffer
// can be overwritten on the next block.
type manifestDeflateCodec struct {
	ocf.Codec
	input  bytes.Reader
	output bytes.Buffer
	reader io.ReadCloser
}

const manifestDeflateShrinkThreshold = 1 << 20

var _ ocf.BoundedDecompressor = (*manifestDeflateCodec)(nil)

func newManifestDeflateCodec() *manifestDeflateCodec {
	return &manifestDeflateCodec{Codec: ocf.DeflateCodec(flate.DefaultCompression)}
}

func (c *manifestDeflateCodec) Decompress(src []byte) ([]byte, error) {
	return c.DecompressBounded(src, 0)
}

func (c *manifestDeflateCodec) DecompressBounded(src []byte, maxBytes int64) ([]byte, error) {
	c.input.Reset(src)
	defer c.input.Reset(nil)

	if c.reader == nil {
		c.reader = flate.NewReader(&c.input)
	} else if err := c.reader.(flate.Resetter).Reset(&c.input, nil); err != nil {
		return nil, err
	}

	var reader io.Reader = c.reader
	if maxBytes > 0 && maxBytes < math.MaxInt64 {
		reader = io.LimitReader(reader, maxBytes+1)
	}
	c.output.Reset()
	size, err := c.output.ReadFrom(reader)
	if err == nil && maxBytes > 0 && size > maxBytes {
		err = fmt.Errorf("ocf: decompressed block exceeds limit of %d bytes (raise WithMaxDecompressedBlockBytes)", maxBytes)
	}
	if err != nil {
		c.output = bytes.Buffer{}

		return nil, err
	}
	if capacity := c.output.Cap(); capacity > manifestDeflateShrinkThreshold && size <= int64(capacity/4) {
		// Copy a small block once so the reader can release the backing array
		// from an earlier, much larger block.
		c.output = *bytes.NewBuffer(bytes.Clone(c.output.Bytes()))
	}

	return c.output.Bytes(), nil
}

func (c *manifestDeflateCodec) Close() error {
	if c.reader != nil {
		// Decompression already returns errors from the inflater.
		_ = c.reader.Close()
		c.reader = nil
	}
	c.output = bytes.Buffer{}

	return nil
}
