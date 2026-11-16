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
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
)

func (c *Catalog) loadNamespace(ctx context.Context, ns table.Identifier) (item, error) {
	if err := validateNamespace(ns, false); err != nil {
		return nil, err
	}
	entry, err := c.get(ctx, namespaceKey(ns))
	if err != nil {
		return nil, err
	}
	if len(entry) == 0 {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns)
	}
	if attrString(entry[versionCol]) == "" {
		return nil, errors.New("DynamoDB namespace version is missing")
	}

	return entry, nil
}

func (c *Catalog) CheckNamespaceExists(ctx context.Context, ns table.Identifier) (bool, error) {
	_, err := c.loadNamespace(ctx, ns)
	if errors.Is(err, catalog.ErrNoSuchNamespace) {
		return false, nil
	}

	return err == nil, err
}

func (c *Catalog) LoadNamespaceProperties(ctx context.Context, ns table.Identifier) (iceberg.Properties, error) {
	entry, err := c.loadNamespace(ctx, ns)
	if err != nil {
		return nil, err
	}

	return properties(entry), nil
}

func (c *Catalog) CreateNamespace(ctx context.Context, ns table.Identifier, props iceberg.Properties) error {
	if err := validateNamespace(ns, false); err != nil {
		return err
	}
	entry := namespaceKey(ns)
	for key, value := range props {
		entry["p."+key] = stringAttr(value)
	}
	setEntryMetadata(entry, true)
	for range 6 {
		writes := []types.TransactWriteItem{{Put: &types.Put{
			TableName: aws.String(c.tableName), Item: entry, ConditionExpression: aws.String("attribute_not_exists(#id)"),
			ExpressionAttributeNames: map[string]string{"#id": identifierCol},
		}}}
		if len(ns) > 1 {
			parent := ns[:len(ns)-1]
			current, err := c.loadNamespace(ctx, parent)
			if err != nil {
				return err
			}
			writes = append(writes, c.touchNamespace(parent, current))
		}
		err := c.transaction(ctx, writes)
		if err == nil {
			return nil
		}
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		current, checkErr := c.get(checkCtx, namespaceKey(ns))
		cancel()
		if checkErr == nil && attrString(current[versionCol]) == attrString(entry[versionCol]) {
			return nil
		}
		if !conditionFailed(err) && !transactionConflict(err) {
			return err
		}
		if len(current) != 0 {
			return fmt.Errorf("%w: %s", catalog.ErrNamespaceAlreadyExists, ns)
		}
	}

	return fmt.Errorf("%w: namespace parent changed during creation", table.ErrCommitFailed)
}

func (c *Catalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	if err := validateNamespace(parent, true); err != nil {
		return nil, err
	}
	if len(parent) != 0 {
		if _, err := c.loadNamespace(ctx, parent); err != nil {
			return nil, err
		}
	}
	prefix := strings.Join(parent, ".")
	if prefix != "" {
		prefix += "."
	}
	input := &dynamodb.QueryInput{
		TableName: aws.String(c.tableName), ConsistentRead: aws.Bool(true),
		KeyConditionExpression:    aws.String("#id = :id"),
		ExpressionAttributeNames:  map[string]string{"#id": identifierCol},
		ExpressionAttributeValues: item{":id": stringAttr(namespaceIdentifier)},
	}
	if prefix != "" {
		input.KeyConditionExpression = aws.String("#id = :id AND begins_with(#ns, :prefix)")
		input.ExpressionAttributeNames["#ns"] = namespaceCol
		input.ExpressionAttributeValues[":prefix"] = stringAttr(prefix)
	}
	namespaces := []table.Identifier{}
	paginator := dynamodb.NewQueryPaginator(c.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, entry := range page.Items {
			ns := strings.Split(attrString(entry[namespaceCol]), ".")
			if len(ns) == len(parent)+1 && slices.Equal(ns[:len(parent)], parent) {
				namespaces = append(namespaces, ns)
			}
		}
	}

	return namespaces, nil
}

func (c *Catalog) DropNamespace(ctx context.Context, ns table.Identifier) error {
	current, err := c.loadNamespace(ctx, ns)
	if err != nil {
		return err
	}
	// Check every descendant, including namespaces written by Java without an
	// intermediate parent entry.
	children, err := c.client.Query(ctx, &dynamodb.QueryInput{
		TableName: aws.String(c.tableName), ConsistentRead: aws.Bool(true), Limit: aws.Int32(1),
		KeyConditionExpression:    aws.String("#id = :id AND begins_with(#ns, :prefix)"),
		ExpressionAttributeNames:  map[string]string{"#id": identifierCol, "#ns": namespaceCol},
		ExpressionAttributeValues: item{":id": stringAttr(namespaceIdentifier), ":prefix": stringAttr(strings.Join(ns, ".") + ".")},
	})
	if err != nil {
		return err
	}
	if len(children.Items) != 0 {
		return catalog.ErrNamespaceNotEmpty
	}

	// The index can cheaply find a table, but may return a recently deleted
	// entry. Confirm it against the base table before rejecting the drop.
	tables, err := c.client.Query(ctx, &dynamodb.QueryInput{
		TableName: aws.String(c.tableName), IndexName: aws.String(namespaceIndex), Limit: aws.Int32(1),
		KeyConditionExpression:    aws.String("#ns = :ns AND begins_with(#id, :prefix)"),
		ExpressionAttributeNames:  map[string]string{"#ns": namespaceCol, "#id": identifierCol},
		ExpressionAttributeValues: item{":ns": stringAttr(strings.Join(ns, ".")), ":prefix": stringAttr(strings.Join(ns, ".") + ".")},
	})
	if err != nil {
		return err
	}
	if len(tables.Items) != 0 {
		entry, err := c.get(ctx, item{identifierCol: tables.Items[0][identifierCol], namespaceCol: tables.Items[0][namespaceCol]})
		if err != nil {
			return err
		}
		if len(entry) != 0 {
			return catalog.ErrNamespaceNotEmpty
		}
	}

	// GSI reads are eventually consistent. Check emptiness through a consistent
	// base-table scan, then guard the delete with the namespace version. Go
	// create/register/rename operations update that version in their transaction.
	scan := dynamodb.NewScanPaginator(c.client, &dynamodb.ScanInput{
		TableName: aws.String(c.tableName), ConsistentRead: aws.Bool(true),
		FilterExpression:          aws.String("#ns = :ns AND #id <> :marker"),
		ExpressionAttributeNames:  map[string]string{"#ns": namespaceCol, "#id": identifierCol},
		ExpressionAttributeValues: item{":ns": stringAttr(strings.Join(ns, ".")), ":marker": stringAttr(namespaceIdentifier)},
	})
	for scan.HasMorePages() {
		page, err := scan.NextPage(ctx)
		if err != nil {
			return err
		}
		if len(page.Items) != 0 {
			return catalog.ErrNamespaceNotEmpty
		}
	}
	err = c.transaction(ctx, []types.TransactWriteItem{{Delete: &types.Delete{
		TableName: aws.String(c.tableName), Key: namespaceKey(ns), ConditionExpression: aws.String("#v = :base"),
		ExpressionAttributeNames: map[string]string{"#v": versionCol}, ExpressionAttributeValues: item{":base": current[versionCol]},
	}}})
	if conditionFailed(err) || transactionConflict(err) {
		return fmt.Errorf("%w: namespace changed during deletion", table.ErrCommitFailed)
	}

	return err
}

func (c *Catalog) UpdateNamespaceProperties(ctx context.Context, ns table.Identifier, removals []string, updates iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	current, err := c.loadNamespace(ctx, ns)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}
	updated, summary, err := internal.GetUpdatedPropsAndUpdateSummary(properties(current), removals, updates)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}
	entry := maps.Clone(current)
	for key := range entry {
		if strings.HasPrefix(key, "p.") {
			delete(entry, key)
		}
	}
	for key, value := range updated {
		entry["p."+key] = stringAttr(value)
	}
	setEntryMetadata(entry, false)
	err = c.transaction(ctx, []types.TransactWriteItem{{Put: &types.Put{
		TableName: aws.String(c.tableName), Item: entry, ConditionExpression: aws.String("#v = :base"),
		ExpressionAttributeNames: map[string]string{"#v": versionCol}, ExpressionAttributeValues: item{":base": current[versionCol]},
	}}})
	if err != nil {
		if conditionFailed(err) || transactionConflict(err) {
			err = fmt.Errorf("%w: namespace properties changed", table.ErrCommitFailed)
		}

		return catalog.PropertiesUpdateSummary{}, err
	}

	return summary, nil
}

func (c *Catalog) touchNamespace(ns table.Identifier, current item) types.TransactWriteItem {
	return types.TransactWriteItem{Update: &types.Update{
		TableName: aws.String(c.tableName), Key: namespaceKey(ns),
		ConditionExpression: aws.String("#v = :base"), UpdateExpression: aws.String("SET #v = :next, #updated = :now"),
		ExpressionAttributeNames:  map[string]string{"#v": versionCol, "#updated": "updated_at"},
		ExpressionAttributeValues: item{":base": current[versionCol], ":next": stringAttr(uuid.NewString()), ":now": &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().UnixMilli(), 10)}},
	}}
}
