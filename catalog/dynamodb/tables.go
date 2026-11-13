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

package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/maintenance"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (c *Catalog) loadTableEntry(ctx context.Context, ident table.Identifier) (item, error) {
	if err := validateIdentifier(ident); err != nil {
		return nil, err
	}
	entry, err := c.get(ctx, tableKey(ident))
	if err != nil {
		return nil, err
	}
	if len(entry) == 0 || !strings.EqualFold(attrString(entry["p.table_type"]), "ICEBERG") {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchTable, ident)
	}
	if attrString(entry[metadataLocationProp]) == "" || attrString(entry[versionCol]) == "" {
		return nil, errors.New("DynamoDB table metadata location or version is missing")
	}

	return entry, nil
}

func (c *Catalog) loadTable(ctx context.Context, ident table.Identifier, entry item) (*table.Table, error) {
	location := attrString(entry[metadataLocationProp])
	reporter, err := c.reporter.Get(c.props)
	if err != nil {
		return nil, err
	}

	return table.NewFromLocation(ctx, ident, location, c.fileSystem(c.props, location), c,
		table.WithMetricsReporter(reporter), table.WithScanPlanningIOProperties(c.props))
}

func (c *Catalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	entry, err := c.loadTableEntry(ctx, ident)
	if err != nil {
		return nil, err
	}

	return c.loadTable(ctx, ident, entry)
}

func (c *Catalog) CheckTableExists(ctx context.Context, ident table.Identifier) (bool, error) {
	_, err := c.loadTableEntry(ctx, ident)
	if errors.Is(err, catalog.ErrNoSuchTable) {
		return false, nil
	}

	return err == nil, err
}

func (c *Catalog) namespaceLocationProperties(ctx context.Context, ns table.Identifier) (iceberg.Properties, error) {
	props, err := c.LoadNamespaceProperties(ctx, ns)
	if err != nil {
		return nil, err
	}
	// Java DynamoDbCatalog uses default_location, rather than the location
	// namespace property used by the generic catalog staging helper.
	props["location"] = props[DefaultLocationKey]

	return props, nil
}

func (c *Catalog) CreateTable(ctx context.Context, ident table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	if err := validateIdentifier(ident); err != nil {
		return nil, err
	}
	if _, err := c.loadNamespace(ctx, catalog.NamespaceFromIdent(ident)); err != nil {
		return nil, err
	}
	staged, err := internal.CreateStagedTable(ctx, c.props, c.namespaceLocationProperties, ident, schema, nil, opts...)
	if err != nil {
		return nil, err
	}
	if err := internal.WriteMetadata(utils.WithAWSConfig(ctx, &c.awsConfig), staged.Table); err != nil {
		return nil, err
	}
	if err := c.createTableEntry(ctx, ident, staged.MetadataLocation()); err != nil {
		return nil, err
	}

	return c.LoadTable(ctx, ident)
}

func newTableEntry(ident table.Identifier, location string) item {
	entry := tableKey(ident)
	entry["p.table_type"] = stringAttr("ICEBERG")
	entry[metadataLocationProp] = stringAttr(location)
	setEntryMetadata(entry, true)

	return entry
}

func (c *Catalog) createTableEntry(ctx context.Context, ident table.Identifier, location string) error {
	entry := newTableEntry(ident, location)
	ns := catalog.NamespaceFromIdent(ident)
	for range 6 {
		namespace, err := c.loadNamespace(ctx, ns)
		if err != nil {
			return err
		}
		err = c.transaction(ctx, []types.TransactWriteItem{
			{Put: &types.Put{
				TableName: aws.String(c.tableName), Item: entry,
				ConditionExpression: aws.String("attribute_not_exists(#id)"), ExpressionAttributeNames: map[string]string{"#id": identifierCol},
			}},
			c.touchNamespace(ns, namespace),
		})
		if err == nil {
			return nil
		}
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		current, checkErr := c.get(checkCtx, tableKey(ident))
		cancel()
		if checkErr == nil && attrString(current[versionCol]) == attrString(entry[versionCol]) {
			return nil
		}
		if !conditionFailed(err) && !transactionConflict(err) {
			return err
		}
		if len(current) != 0 {
			return fmt.Errorf("%w: %s", catalog.ErrTableAlreadyExists, ident)
		}
	}

	return fmt.Errorf("%w: namespace changed while creating table", table.ErrCommitFailed)
}

func (c *Catalog) RegisterTable(ctx context.Context, ident table.Identifier, location string) (*table.Table, error) {
	if err := validateIdentifier(ident); err != nil {
		return nil, err
	}
	if _, err := c.loadNamespace(ctx, catalog.NamespaceFromIdent(ident)); err != nil {
		return nil, err
	}
	if _, err := table.NewFromLocation(ctx, ident, location, c.fileSystem(c.props, location), c); err != nil {
		return nil, err
	}
	if err := c.createTableEntry(ctx, ident, location); err != nil {
		return nil, err
	}

	return c.LoadTable(ctx, ident)
}

func (c *Catalog) ListTables(ctx context.Context, ns table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		if _, err := c.loadNamespace(ctx, ns); err != nil {
			yield(nil, err)

			return
		}
		paginator := dynamodb.NewQueryPaginator(c.client, &dynamodb.QueryInput{
			TableName: aws.String(c.tableName), IndexName: aws.String(namespaceIndex),
			KeyConditionExpression: aws.String("#ns = :ns"), ExpressionAttributeNames: map[string]string{"#ns": namespaceCol},
			ExpressionAttributeValues: item{":ns": stringAttr(strings.Join(ns, "."))},
		})
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				yield(nil, err)

				return
			}
			for _, entry := range page.Items {
				identifier := attrString(entry[identifierCol])
				if identifier == namespaceIdentifier {
					continue
				}
				ident := strings.Split(identifier, ".")
				if err := validateIdentifier(ident); err != nil {
					yield(nil, err)

					return
				}
				if !yield(ident, nil) {
					return
				}
			}
		}
	}
}

func (c *Catalog) CommitTable(ctx context.Context, ident table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	entry, err := c.loadTableEntry(ctx, ident)
	if err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, "", err
	}
	var current *table.Table
	if len(entry) != 0 {
		current, err = c.loadTable(ctx, ident, entry)
		if err != nil {
			return nil, "", err
		}
	}
	staged, err := internal.UpdateAndStageTable(ctx, c.props, current, ident, requirements, updates, c, nil)
	if err != nil {
		return nil, "", err
	}
	if current != nil && staged.Metadata().Equals(current.Metadata()) {
		return current.Metadata(), current.MetadataLocation(), nil
	}
	if err := internal.WriteMetadata(utils.WithAWSConfig(ctx, &c.awsConfig), staged.Table); err != nil {
		return nil, "", err
	}
	if current == nil {
		if err := c.createTableEntry(ctx, ident, staged.MetadataLocation()); err != nil {
			return nil, "", err
		}
	} else {
		updated := maps.Clone(entry)
		updated["p.previous_metadata_location"] = entry[metadataLocationProp]
		updated[metadataLocationProp] = stringAttr(staged.MetadataLocation())
		setEntryMetadata(updated, false)
		err := c.transaction(ctx, []types.TransactWriteItem{{Put: &types.Put{
			TableName: aws.String(c.tableName), Item: updated, ConditionExpression: aws.String("#v = :base"),
			ExpressionAttributeNames: map[string]string{"#v": versionCol}, ExpressionAttributeValues: item{":base": entry[versionCol]},
		}}})
		if err != nil {
			committed, statusErr := c.commitSucceeded(ctx, ident, staged.MetadataLocation())
			if !committed {
				if conditionFailed(err) || transactionConflict(err) {
					err = fmt.Errorf("%w: table metadata changed: %w", table.ErrCommitFailed, err)
					if statusErr == nil {
						if cleanupErr := c.removeUncommittedMetadata(ctx, staged.MetadataLocation(), staged.FS); cleanupErr != nil {
							err = errors.Join(err, cleanupErr)
						}
					}
				}

				// Keep the file if the commit outcome could not be determined.
				return nil, "", err
			}
		}
	}

	return staged.Metadata(), staged.MetadataLocation(), nil
}

func (c *Catalog) commitSucceeded(ctx context.Context, ident table.Identifier, location string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	loaded, err := c.LoadTable(ctx, ident)
	if err != nil {
		return false, err
	}
	if loaded.MetadataLocation() == location {
		return true, nil
	}
	for entry := range loaded.Metadata().PreviousFiles() {
		if entry.MetadataFile == location {
			return true, nil
		}
	}

	return false, nil
}

func (c *Catalog) removeUncommittedMetadata(ctx context.Context, location string, loadFS table.FSysF) error {
	cleanupCtx, cancel := context.WithTimeout(utils.WithAWSConfig(context.WithoutCancel(ctx), &c.awsConfig), 5*time.Second)
	defer cancel()
	fs, err := loadFS(cleanupCtx)
	if err != nil {
		return fmt.Errorf("load filesystem to remove uncommitted metadata %s: %w", location, err)
	}
	if err := fs.Remove(cleanupCtx, location); err != nil {
		return fmt.Errorf("remove uncommitted metadata %s: %w", location, err)
	}

	return nil
}

func (c *Catalog) dropEntry(ctx context.Context, ident table.Identifier, entry item) error {
	err := c.transaction(ctx, []types.TransactWriteItem{{Delete: &types.Delete{
		TableName: aws.String(c.tableName), Key: tableKey(ident), ConditionExpression: aws.String("#v = :base"),
		ExpressionAttributeNames: map[string]string{"#v": versionCol}, ExpressionAttributeValues: item{":base": entry[versionCol]},
	}}})
	if conditionFailed(err) || transactionConflict(err) {
		return fmt.Errorf("%w: table changed while dropping: %w", table.ErrCommitFailed, err)
	}

	return err
}

func (c *Catalog) DropTable(ctx context.Context, ident table.Identifier) error {
	entry, err := c.loadTableEntry(ctx, ident)
	if err != nil {
		return err
	}

	return c.dropEntry(ctx, ident, entry)
}

func (c *Catalog) PurgeTable(ctx context.Context, ident table.Identifier) error {
	entry, err := c.loadTableEntry(ctx, ident)
	if err != nil {
		return err
	}
	tbl, err := c.loadTable(ctx, ident, entry)
	if err != nil {
		return err
	}
	if err := c.dropEntry(ctx, ident, entry); err != nil {
		return err
	}
	if err := maintenance.PurgeFiles(ctx, tbl); err != nil {
		log.Printf("WARNING: dropped table %s but failed to purge files: %v", ident, err)
	}

	return nil
}

func (c *Catalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	if err := validateIdentifier(to); err != nil {
		return nil, err
	}
	source, err := c.loadTableEntry(ctx, from)
	if err != nil {
		return nil, err
	}
	if slices.Equal(from, to) {
		return c.loadTable(ctx, from, source)
	}
	ns := catalog.NamespaceFromIdent(to)
	namespace, err := c.loadNamespace(ctx, ns)
	if err != nil {
		return nil, err
	}
	destination := maps.Clone(source)
	maps.Copy(destination, tableKey(to))
	setEntryMetadata(destination, true)
	err = c.transaction(ctx, []types.TransactWriteItem{
		{Delete: &types.Delete{
			TableName: aws.String(c.tableName), Key: tableKey(from), ConditionExpression: aws.String("#v = :base"),
			ExpressionAttributeNames: map[string]string{"#v": versionCol}, ExpressionAttributeValues: item{":base": source[versionCol]},
		}},
		{Put: &types.Put{
			TableName: aws.String(c.tableName), Item: destination, ConditionExpression: aws.String("attribute_not_exists(#id)"),
			ExpressionAttributeNames: map[string]string{"#id": identifierCol},
		}},
		c.touchNamespace(ns, namespace),
	})
	if err != nil {
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		current, checkErr := c.get(checkCtx, tableKey(to))
		if checkErr == nil && attrString(current[versionCol]) == attrString(destination[versionCol]) {
			return c.loadTable(checkCtx, to, current)
		}
		if conditionFailed(err) || transactionConflict(err) {
			if len(current) != 0 {
				return nil, catalog.ErrTableAlreadyExists
			}

			return nil, fmt.Errorf("%w: table or namespace changed during rename: %w", table.ErrCommitFailed, err)
		}

		return nil, err
	}

	return c.LoadTable(ctx, to)
}
