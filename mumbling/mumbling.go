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

// Package mumbling decodes the Mumbling compressed-bitmap format
// (format/mumbling-spec.md, version 1) used for bounded-size deletion vectors.
package mumbling

import (
	"errors"
	"fmt"
)

// Sentinel errors returned when decoding, for use with errors.Is.
var (
	// ErrTruncated means the buffer is shorter than the structure it declares.
	ErrTruncated = errors.New("mumbling: truncated buffer")
	// ErrMalformed means the bytes are present but invalid.
	ErrMalformed = errors.New("mumbling: malformed data")
	// ErrCapExceeded means a header value is above the spec cap.
	ErrCapExceeded = errors.New("mumbling: spec cap exceeded")
)

const (
	version           = 1
	headerSize        = 6
	denseContainerBit = 0x20 // MumblingBitmap.java:38
	chunkSize         = 256  // PFOREncoding.java
	denseBytes        = 32

	// Spec caps: at most 8192 containers and 8192*256 = 2,097,152 set bits.
	maxContainers  = 8192
	maxCardinality = 8192 * 256
)

// Bitmap is a read-only view over a Mumbling-encoded byte slice. Decoding is
// lazy: descriptors and container offsets are built on the first IsSet call.
type Bitmap struct {
	data           []byte
	cardinality    int
	containerCount int
	descriptors    []int // nil until decoded
	offsets        []int // len = containerCount+1
}

// New parses the 6-byte header and returns a lazy Bitmap.
func New(data []byte) (*Bitmap, error) {
	if len(data) < headerSize {
		return nil, fmt.Errorf("%w: %d < %d", ErrTruncated, len(data), headerSize)
	}
	if v := int(data[0]); v != version {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrMalformed, v)
	}
	card := int(data[1]) | int(data[2])<<8 | int(data[3])<<16 // 24-bit LE
	count := int(data[4]) | int(data[5])<<8                   // 16-bit LE
	if count > maxContainers {
		return nil, fmt.Errorf("%w: container count %d > %d", ErrCapExceeded, count, maxContainers)
	}
	if card > maxCardinality {
		return nil, fmt.Errorf("%w: cardinality %d > %d", ErrCapExceeded, card, maxCardinality)
	}

	return &Bitmap{data: data, cardinality: card, containerCount: count}, nil
}

// Cardinality returns the number of set bits (from the header).
func (b *Bitmap) Cardinality() int { return b.cardinality }

// IsSet reports whether bit pos is set.
func (b *Bitmap) IsSet(pos int) (bool, error) {
	if pos < 0 {
		return false, fmt.Errorf("mumbling: invalid bit position: %d", pos)
	}
	containerIndex := pos >> 8
	posInContainer := pos & 0xFF
	if containerIndex >= b.containerCount {
		return false, nil
	}
	if b.descriptors == nil {
		if err := b.decodeDescriptors(); err != nil {
			return false, err
		}
	}
	start := b.offsets[containerIndex]
	desc := b.descriptors[containerIndex]

	if desc&denseContainerBit == denseContainerBit {
		byteIndex := posInContainer >> 3
		bitShift := 7 - (posInContainer & 0x7) // MSB-first within byte
		if start+byteIndex >= len(b.data) {
			return false, fmt.Errorf("%w: dense container at %d", ErrTruncated, start+byteIndex)
		}

		return (b.data[start+byteIndex]>>uint(bitShift))&1 == 1, nil
	}
	// sparse: sorted position bytes, one per set bit; desc == length
	for i := 0; i < desc; i++ {
		if start+i >= len(b.data) {
			return false, fmt.Errorf("%w: sparse container at %d", ErrTruncated, start+i)
		}
		stored := int(b.data[start+i])
		if stored == posInContainer {
			return true, nil
		}
		if stored > posInContainer {
			return false, nil
		}
	}

	return false, nil
}

// decodeDescriptors PFOR-decodes the descriptor array and builds absolute
// container offsets.
func (b *Bitmap) decodeDescriptors() error {
	descs := make([]int, b.containerCount)
	read, err := decodeChunks(b.data, headerSize, descs, b.containerCount)
	if err != nil {
		return err
	}
	offsets := make([]int, b.containerCount+1)
	offsets[0] = headerSize + read
	for i, d := range descs {
		if d&denseContainerBit == denseContainerBit {
			offsets[i+1] = offsets[i] + denseBytes
		} else {
			offsets[i+1] = offsets[i] + d
		}
	}
	b.descriptors, b.offsets = descs, offsets

	return nil
}

// decodeChunks decodes count values starting at offset, returning bytes read.
func decodeChunks(data []byte, offset int, out []int, count int) (int, error) {
	read, done := 0, 0
	for done < count {
		n := count - done
		if n > chunkSize {
			n = chunkSize
		}
		cb, err := decodeChunk(data, offset+read, out[done:done+n], n)
		if err != nil {
			return 0, err
		}
		read += cb
		done += n
	}

	return read, nil
}

// decodeChunk decodes one PFOR chunk.
func decodeChunk(data []byte, off int, out []int, count int) (int, error) {
	if off+3 > len(data) {
		return 0, fmt.Errorf("%w: chunk header at %d", ErrTruncated, off)
	}
	b1 := int(data[off]) & 0x0F
	b2 := (int(data[off]) >> 4) & 0x0F
	excCount := int(data[off+1])
	base := int(data[off+2])
	read := 3

	pb, err := unpackBits(data, off+read, b1, out[:count])
	if err != nil {
		return 0, err
	}
	read += pb

	if excCount > 0 {
		excOffsets := make([]int, excCount)
		excValues := make([]int, excCount)
		ob, err := unpackBits(data, off+read, 8, excOffsets)
		if err != nil {
			return 0, err
		}
		read += ob
		vb, err := unpackBits(data, off+read, b2, excValues)
		if err != nil {
			return 0, err
		}
		read += vb
		for i := 0; i < excCount; i++ {
			if excOffsets[i] >= count {
				return 0, fmt.Errorf("%w: exception offset %d out of range (chunk len %d)", ErrMalformed, excOffsets[i], count)
			}
			out[excOffsets[i]] |= excValues[i] << uint(b1)
		}
	}
	for i := 0; i < count; i++ {
		out[i] += base
	}

	return read, nil
}

// unpackBits reads len(out) values of width bits each, MSB-first, returning bytes consumed.
func unpackBits(data []byte, off, width int, out []int) (int, error) {
	if width == 0 {
		for i := range out {
			out[i] = 0
		}

		return 0, nil
	}
	byteWidth := (len(out)*width + 7) / 8
	if off+byteWidth > len(data) {
		return 0, fmt.Errorf("%w: bit-packed section at %d", ErrTruncated, off)
	}
	mask := (1 << uint(width)) - 1
	bitPos := 0
	for i := range out {
		v := 0
		for k := 0; k < width; k++ {
			byteIdx := off + (bitPos >> 3)
			bitIdx := 7 - (bitPos & 7)
			v = (v << 1) | int((data[byteIdx]>>uint(bitIdx))&1)
			bitPos++
		}
		out[i] = v & mask
	}

	return byteWidth, nil
}
