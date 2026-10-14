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

package table

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func sampleHeapInuse() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return m.HeapInuse
}

func BenchmarkArrowScanReorderHeapLaggingTask(b *testing.B) {
	const (
		rowsPerFile    = 2048
		numWorkers     = 4
		sampleInterval = 5 * time.Millisecond
		idleBeforeOpen = 100 * time.Millisecond
	)

	for _, files := range []int{8, 32, 128} {
		b.Run(fmt.Sprintf("files_%d", files), func(b *testing.B) {
			dir := filepath.ToSlash(b.TempDir())
			gateIO := &reorderGateIO{}
			tbl, tasks := newReorderScanFixture(b, dir, files, rowsPerFile, gateIO)
			headPath := tasks[0].File.FilePath()
			ctx := context.Background()

			baseline := sampleHeapInuse()
			var (
				peakDelta    float64
				gatedBatches int
			)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				closes := make(chan struct{}, files)
				gateIO.arm(headPath, closes)

				_, records, err := tbl.Scan(WithMaxConcurrency(numWorkers)).ReadTasks(ctx, tasks)
				if err != nil {
					b.Fatal(err)
				}

				done := make(chan error, 1)
				go func() {
					var rows int64
					for rec, err := range records {
						if err != nil {
							done <- err

							return
						}
						rows += rec.NumRows()
						rec.Release()
					}
					if rows != int64(files*rowsPerFile) {
						done <- fmt.Errorf("read %d rows, want %d", rows, files*rowsPerFile)

						return
					}
					done <- nil
				}()

				stopSampler := make(chan struct{})
				peakCh := make(chan uint64, 1)
				go func() {
					ticker := time.NewTicker(sampleInterval)
					defer ticker.Stop()
					var peak uint64
					for {
						select {
						case <-stopSampler:
							peakCh <- max(peak, sampleHeapInuse())

							return
						case <-ticker.C:
							peak = max(peak, sampleHeapInuse())
						}
					}
				}()

				closed := 0
				idle := time.NewTimer(idleBeforeOpen)
			gated:
				for closed < files-1 {
					select {
					case <-closes:
						closed++
						idle.Reset(idleBeforeOpen)
					case <-idle.C:
						break gated
					}
				}
				idle.Stop()

				close(stopSampler)
				peak := <-peakCh
				gateIO.release()

				if err := <-done; err != nil {
					b.Fatal(err)
				}

				if peak > baseline {
					peakDelta = max(peakDelta, float64(peak-baseline)/(1024*1024))
				}
				gatedBatches = max(gatedBatches, closed)
			}

			b.ReportMetric(peakDelta, "peak-heap-delta-MB")
			b.ReportMetric(float64(gatedBatches), "gated-batches")
		})
	}
}
