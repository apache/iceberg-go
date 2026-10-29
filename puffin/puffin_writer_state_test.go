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

package puffin

import (
	"errors"
	"io"
	"testing"
)

func TestWriterStateGuardErrors(t *testing.T) {
	ops := []struct {
		name   string
		action string
		call   func(*Writer) error
	}{
		{"SetProperties", "set properties", func(w *Writer) error {
			return w.SetProperties(map[string]string{"key": "value"})
		}},
		{"ClearProperties", "clear properties", func(w *Writer) error {
			return w.ClearProperties()
		}},
		{"SetCreatedBy", "set created-by", func(w *Writer) error {
			return w.SetCreatedBy("")
		}},
		{"AddBlob", "add blob", func(w *Writer) error {
			_, err := w.AddBlob(BlobMetadataInput{}, nil)
			return err
		}},
		{"Finish", "finish", func(w *Writer) error {
			return w.Finish()
		}},
	}

	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			finalized, err := NewWriter(io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if err := finalized.Finish(); err != nil {
				t.Fatal(err)
			}

			wantFinalized := "puffin: cannot " + op.action + ": writer already finalized"
			if got := op.call(finalized); got == nil || got.Error() != wantFinalized {
				t.Fatalf("finalized: got %v, want %q", got, wantFinalized)
			}

			failed, err := NewWriter(io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			underlying := errors.New("write failed")
			failed.failed = underlying
			wantFailed := "puffin: cannot " + op.action + ": writer previously failed: write failed"
			if got := op.call(failed); got == nil || got.Error() != wantFailed || !errors.Is(got, underlying) {
				t.Fatalf("failed: got %v, want error wrapping %q", got, wantFailed)
			}

			// When both flags are set, finalization still takes precedence.
			failed.done = true
			if got := op.call(failed); got == nil || got.Error() != wantFinalized {
				t.Fatalf("finalized and failed: got %v, want %q", got, wantFinalized)
			}
		})
	}
}
