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

package catalog

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/apache/iceberg-go/table"
)

// MultiTableTransaction collects changes across multiple tables and
// commits them atomically via [TransactionalCatalog.CommitTransaction].
//
// A MultiTableTransaction must not be used concurrently from multiple
// goroutines.
//
// Usage:
//
//	mtx, err := catalog.NewMultiTableTransaction(cat)
//	// ... err check ...
//
//	tx1 := tbl1.NewTransaction()
//	tx1.SetProperties(map[string]string{"key": "val"})
//	mtx.AddTransaction(tx1)
//
//	tx2 := tbl2.NewTransaction()
//	// ... build changes on tx2 ...
//	mtx.AddTransaction(tx2)
//
//	err = mtx.Commit(ctx)
type MultiTableTransaction struct {
	cat       TransactionalCatalog
	txns      []*table.Transaction
	idents    []string
	committed bool
}

// NewMultiTableTransaction creates a new multi-table transaction backed
// by the given catalog. Returns an error if the catalog does not
// implement [TransactionalCatalog].
func NewMultiTableTransaction(cat Catalog) (*MultiTableTransaction, error) {
	tc, ok := cat.(TransactionalCatalog)
	if !ok {
		return nil, errors.New("catalog does not support multi-table transactions")
	}

	return &MultiTableTransaction{cat: tc}, nil
}

// AddTransaction adds a table transaction to be committed atomically
// with all other transactions in this multi-table transaction.
// Returns an error if the transaction is nil, already committed, or
// targets a table that was already added.
func (m *MultiTableTransaction) AddTransaction(tx *table.Transaction) error {
	if tx == nil {
		return errors.New("transaction must not be nil")
	}

	if m.committed {
		return errors.New("multi-table transaction has already been committed")
	}

	tc, err := tx.TableCommit()
	if err != nil {
		return err
	}

	key := strings.Join(tc.Identifier, ".")
	if slices.Contains(m.idents, key) {
		return fmt.Errorf("duplicate table in multi-table transaction: %s", key)
	}

	m.txns = append(m.txns, tx)
	m.idents = append(m.idents, key)

	return nil
}

// Commit extracts pending changes from all added transactions and
// commits them atomically. On success, all transactions are marked
// as committed. On failure, no transactions are marked committed
// and the caller may retry.
//
// PostCommit hooks are not executed. Because the multi-table commit
// endpoint returns 204 No Content, callers must LoadTable individually
// to obtain updated metadata.
func (m *MultiTableTransaction) Commit(ctx context.Context) error {
	if m.committed {
		return errors.New("multi-table transaction has already been committed")
	}

	if len(m.txns) == 0 {
		return ErrEmptyCommitList
	}

	commits := make([]table.TableCommit, 0, len(m.txns))
	for _, tx := range m.txns {
		tc, err := tx.TableCommit()
		if err != nil {
			return err
		}

		commits = append(commits, tc)
	}

	if err := m.cat.CommitTransaction(ctx, commits); err != nil {
		return err
	}

	m.committed = true

	// Mark all transactions as committed to prevent reuse.
	for _, tx := range m.txns {
		tx.MarkCommitted()
	}

	return nil
}
