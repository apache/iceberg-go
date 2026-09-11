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
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
	"golang.org/x/sync/errgroup"
)

const (
	lazyDVBenchmarkGroupCount  = 100
	lazyDVBenchmarkDVsPerGroup = 10
	lazyDVBenchmarkTotalDVs    = lazyDVBenchmarkGroupCount * lazyDVBenchmarkDVsPerGroup
)

type lazyDVBenchmarkFixture struct {
	tasks       []FileScanTask
	groupStarts []string
}

type countingLazyDVOpenIO struct {
	iceio.LocalFS
	opens atomic.Int64
}

func (f *countingLazyDVOpenIO) Open(name string) (iceio.File, error) {
	file, err := f.LocalFS.Open(name)
	if err == nil {
		f.opens.Add(1)
	}

	return file, err
}

func newLazyDVBenchmarkFixture(b *testing.B) lazyDVBenchmarkFixture {
	b.Helper()

	baseFS := iceio.LocalFS{}
	root := b.TempDir()
	fixture := lazyDVBenchmarkFixture{
		tasks:       make([]FileScanTask, 0, lazyDVBenchmarkTotalDVs),
		groupStarts: make([]string, 0, lazyDVBenchmarkGroupCount),
	}

	for groupIndex := range lazyDVBenchmarkGroupCount {
		path := filepath.Join(root, fmt.Sprintf("dv-%03d.puffin", groupIndex))
		writer := dv.NewDVWriter(baseFS, func(int32) *iceberg.PartitionSpec {
			return iceberg.UnpartitionedSpec
		})

		for offset := range lazyDVBenchmarkDVsPerGroup {
			dataIndex := groupIndex*lazyDVBenchmarkDVsPerGroup + offset
			ref := fmt.Sprintf("file:///benchmark/data/data-%04d.parquet", dataIndex)
			if offset == 0 {
				fixture.groupStarts = append(fixture.groupStarts, ref)
			}
			if err := writer.Add(ref, []int64{int64(offset)}, 0, nil); err != nil {
				b.Fatal(err)
			}
		}

		files, err := writer.Flush(context.Background(), path)
		if err != nil {
			b.Fatal(err)
		}
		for _, file := range files {
			fixture.tasks = append(fixture.tasks, FileScanTask{
				DeletionVectorFiles: []iceberg.DataFile{file},
			})
		}
	}

	return fixture
}

func BenchmarkLazyDeletionVectorLoading(b *testing.B) {
	fixture := newLazyDVBenchmarkFixture(b)
	fs := &countingLazyDVOpenIO{}

	benchmark := func(name string, load func(*lazyDeletionVectorLoader) error) {
		b.Run(name, func(b *testing.B) {
			fs.opens.Store(0)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				loader, err := newLazyDeletionVectorLoader(fs, fixture.tasks)
				if err != nil {
					b.Fatal(err)
				}
				if err := load(loader); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(fs.opens.Load())/float64(b.N), "puffin-opens/op")
		})
	}

	benchmark("lazy_unread", func(loader *lazyDeletionVectorLoader) error {
		if len(loader.byDataFile) != lazyDVBenchmarkTotalDVs {
			return fmt.Errorf("got %d indexed data files, expected %d", len(loader.byDataFile), lazyDVBenchmarkTotalDVs)
		}

		return nil
	})

	benchmark("lazy_first_group", func(loader *lazyDeletionVectorLoader) error {
		_, err := loader.load(b.Context(), fixture.groupStarts[0])

		return err
	})

	benchmark("lazy_ten_groups", func(loader *lazyDeletionVectorLoader) error {
		return loadLazyDVReferences(b.Context(), loader, fixture.groupStarts[:10])
	})

	benchmark("lazy_full_scan", func(loader *lazyDeletionVectorLoader) error {
		return loadLazyDVReferences(b.Context(), loader, fixture.groupStarts)
	})
}

func loadLazyDVReferences(ctx context.Context, loader *lazyDeletionVectorLoader, references []string) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(16)
	for _, ref := range references {
		g.Go(func() error {
			_, err := loader.load(gctx, ref)

			return err
		})
	}

	return g.Wait()
}
