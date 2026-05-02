// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

package table

import (
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
)

// TestManifestEvalVisitorEvalRace fans out 256 goroutines that all call the
// same cached Eval closure concurrently. Under -race, any write to a shared
// receiver field inside Eval (e.g. m.partitionFields = parts) plus a read
// inside VisitGreater/VisitLess/etc. will trip the detector.
//
// This reproduces the same shared-receiver pattern fixed previously in
// exprEvaluator: newManifestEvaluator returns (&manifestEvalVisitor{...}).Eval,
// callers in transaction.go's classifyFilesForFilteredDeletions loop the
// returned closure across an errgroup of goroutines, and every goroutine
// races on the captured *manifestEvalVisitor.
func TestManifestEvalVisitorEvalRace(t *testing.T) {
	intMin := []byte{0x00, 0x00, 0x00, 0x00}
	intMax := []byte{0x7f, 0x00, 0x00, 0x00}

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID: 1, Name: "id",
			Type: iceberg.PrimitiveTypes.Int32, Required: false,
		},
	)

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		Name:      "id",
		SourceIDs: []int{1},
		FieldID:   1000,
		Transform: iceberg.IdentityTransform{},
	})

	// Single shared evaluator — this is the analogue of
	// manifestEvaluators.Get(specID) returning the same cached closure to
	// every goroutine in the production callers.
	eval, err := newManifestEvaluator(spec, schema,
		iceberg.LessThan(iceberg.Reference("id"), int32(50)),
		true)
	if err != nil {
		t.Fatalf("newManifestEvaluator: %v", err)
	}

	const goroutines = 256

	manifests := make([]iceberg.ManifestFile, goroutines)
	for i := range manifests {
		manifests[i] = iceberg.NewManifestFile(1, "", 0, 0, int64(i)).Partitions(
			[]iceberg.FieldSummary{
				{
					ContainsNull: false,
					LowerBound:   &intMin,
					UpperBound:   &intMax,
				},
			},
		).Build()
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			if _, err := eval(manifests[i]); err != nil {
				t.Errorf("eval[%d]: %v", i, err)
			}
		}()
	}
	close(start)
	wg.Wait()
}
