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

package mumbling

import "math/bits"

// pforEncode encodes unsigned byte values (0-255) with the Mumbling PFOR codec.
func pforEncode(values []int) []byte {
	out := make([]byte, estimateEncodedSize(len(values)))
	n := encode(values, 0, out, 0, len(values))

	return out[:n]
}

// pforDecode decodes count values produced by pforEncode. Thin wrapper over the
// internal chunked decoder.
func pforDecode(data []byte, count int) ([]int, error) {
	out := make([]int, count)
	if _, err := decodeChunks(data, 0, out, count); err != nil {
		return nil, err
	}

	return out, nil
}

func encode(values []int, valueOffset int, out []byte, outOffset, count int) int {
	written, done := 0, 0
	for done < count {
		cl := count - done
		if cl > chunkSize {
			cl = chunkSize
		}
		written += encodeChunk(values, valueOffset+done, out, outOffset+written, cl)
		done += cl
	}

	return written
}

func encodeChunk(values []int, valueOffset int, out []byte, outOffset, count int) int {
	base := minVal(values, valueOffset, count)
	normalized := make([]int, count)
	normSetBits := 0
	for i := 0; i < count; i++ {
		normalized[i] = values[valueOffset+i] - base
		normSetBits |= normalized[i]
	}
	maxWidth := width(normSetBits)
	b1, excCount := chooseBitWidth(normalized, count, maxWidth)
	b2 := maxWidth - b1

	// b1==8: store original values raw with b2=e=m=0. (PFOREncoding.java:200-203)
	if b1 == 8 {
		writeHeader(out, outOffset, 8, 0, 0, 0)

		return 3 + packBits(out, outOffset+3, 8, values[valueOffset:valueOffset+count])
	}

	written := writeHeader(out, outOffset, b1, b2, excCount, base)
	written += packBits(out, outOffset+written, b1, normalized)

	if excCount > 0 {
		excOffsets := make([]int, 0, excCount)
		excValues := make([]int, 0, excCount)
		threshold := 1 << uint(b1)
		for i := 0; i < count; i++ {
			if normalized[i] >= threshold {
				excOffsets = append(excOffsets, i)
				excValues = append(excValues, normalized[i]>>uint(b1))
			}
		}
		written += packBits(out, outOffset+written, 8, excOffsets)
		written += packBits(out, outOffset+written, b2, excValues)
	}

	return written
}

// chooseBitWidth mirrors PFOREncoding.chooseBitWidth: pick the primary width
// minimizing chunk size, preferring the larger width on ties (<=).
func chooseBitWidth(normalized []int, length, maxWidth int) (int, int) {
	bestWidth, bestExc := 0, 0
	bestSize := int(^uint(0) >> 1)
	for cw := 0; cw <= maxWidth; cw++ {
		exc := 0
		if cw < 8 {
			threshold := 1 << uint(cw)
			for i := 0; i < length; i++ {
				if normalized[i] >= threshold {
					exc++
				}
			}
		}
		b2 := maxWidth - cw
		size := byteWidth(length*cw) + exc + byteWidth(exc*b2)
		if size <= bestSize { // <= : larger width wins ties
			bestSize, bestWidth, bestExc = size, cw, exc
		}
	}

	return bestWidth, bestExc
}

func writeHeader(out []byte, off, b1, b2, excCount, base int) int {
	out[off] = byte((b2 << 4) | (b1 & 0x0F))
	out[off+1] = byte(excCount)
	out[off+2] = byte(base)

	return 3
}

// packBits writes len(values) values of width bits each, MSB-first, into out at
// off; returns bytes written.
func packBits(out []byte, off, width int, values []int) int {
	if width == 0 {
		return 0
	}
	bitPos := 0
	for _, v := range values {
		for k := width - 1; k >= 0; k-- {
			bit := (v >> uint(k)) & 1
			byteIdx := off + (bitPos >> 3)
			bitIdx := 7 - (bitPos & 7)
			out[byteIdx] |= byte(bit << uint(bitIdx))
			bitPos++
		}
	}

	return byteWidth(len(values) * width)
}

func minVal(values []int, start, length int) int {
	m := 256
	for i := start; i < start+length; i++ {
		if values[i] < m {
			m = values[i]
		}
	}

	return m
}

func width(v int) int     { return bits.Len(uint(v)) } // 0 for v==0
func byteWidth(b int) int { return (b + 7) / 8 }

func estimateEncodedSize(n int) int {
	return 3*((n+chunkSize-1)/chunkSize) + n
}
