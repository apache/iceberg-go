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

// Package dynamodb implements the Java-compatible AWS DynamoDB catalog.
// Import it to register the "dynamodb" catalog type.
package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/metrics"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
)

const (
	TableNameKey         = "dynamodb.table-name"
	EndpointKey          = "dynamodb.endpoint"
	DefaultLocationKey   = "default_location"
	identifierCol        = "identifier"
	namespaceCol         = "namespace"
	versionCol           = "v"
	namespaceIdentifier  = "NAMESPACE"
	namespaceIndex       = "namespace-identifier"
	metadataLocationProp = "p.metadata_location"
)

type item = map[string]types.AttributeValue

// Catalog stores metadata pointers and namespaces in one DynamoDB table.
// The backing table layout is shared with Java's DynamoDbCatalog. Namespace
// emptiness checks use consistent reads; concurrent namespace deletion is
// coordinated with Go writers, while Java writers retain Java's weaker behavior.
type Catalog struct {
	name      string
	tableName string
	props     iceberg.Properties
	awsConfig aws.Config
	client    *dynamodb.Client
	reporter  metrics.CachedReporter
}

var (
	_ catalog.Catalog             = (*Catalog)(nil)
	_ catalog.RegisterableCatalog = (*Catalog)(nil)
)

func init() {
	catalog.MustRegister("dynamodb", catalog.RegistrarFunc(func(ctx context.Context, name string, props iceberg.Properties) (catalog.Catalog, error) {
		var cfg aws.Config
		if supplied := utils.GetAWSConfig(ctx); supplied != nil {
			cfg = supplied.Copy()
		} else {
			var err error
			cfg, err = config.LoadDefaultConfig(ctx)
			if err != nil {
				return nil, err
			}
		}
		if region := props["client.region"]; region != "" {
			cfg.Region = region
		}

		return NewCatalog(ctx, name, cfg, props)
	}))
}

// NewCatalog opens a catalog and creates its backing DynamoDB table if absent.
// cfg supplies AWS credentials, region, and transport. The warehouse property is
// required; dynamodb.table-name defaults to "iceberg". Options configure the SDK
// service client, for example an endpoint for DynamoDB Local.
func NewCatalog(ctx context.Context, name string, cfg aws.Config, props iceberg.Properties, options ...func(*dynamodb.Options)) (*Catalog, error) {
	if strings.TrimSpace(props["warehouse"]) == "" {
		return nil, errors.New("DynamoDB catalog requires a warehouse")
	}
	if endpoint := props[EndpointKey]; endpoint != "" {
		options = append(append([]func(*dynamodb.Options){}, options...), func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(endpoint) })
	}
	c := &Catalog{name: name, tableName: props.Get(TableNameKey, "iceberg"), props: maps.Clone(props), awsConfig: cfg.Copy(), client: dynamodb.NewFromConfig(cfg, options...)}
	if _, err := c.reporter.Get(props); err != nil {
		return nil, err
	}
	if err := c.ensureTable(ctx); err != nil {
		return nil, errors.Join(err, c.reporter.Close())
	}

	return c, nil
}

func (c *Catalog) Name() string            { return c.name }
func (*Catalog) CatalogType() catalog.Type { return catalog.DynamoDB }
func (c *Catalog) Close() error            { return c.reporter.Close() }

func (c *Catalog) ensureTable(ctx context.Context) error {
	result, err := c.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(c.tableName)})
	if _, missing := errors.AsType[*types.ResourceNotFoundException](err); missing {
		_, err = c.client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String(c.tableName), BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(identifierCol), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(namespaceCol), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(identifierCol), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(namespaceCol), KeyType: types.KeyTypeRange},
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
				IndexName: aws.String(namespaceIndex),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String(namespaceCol), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String(identifierCol), KeyType: types.KeyTypeRange},
				}, Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
			}},
		})
		if _, inUse := errors.AsType[*types.ResourceInUseException](err); err != nil && !inUse {
			return err
		}
	} else if err != nil {
		return err
	}
	if result == nil || result.Table == nil || result.Table.TableStatus != types.TableStatusActive {
		if err := dynamodb.NewTableExistsWaiter(c.client).Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(c.tableName)}, 2*time.Minute); err != nil {
			return err
		}
		result, err = c.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(c.tableName)})
		if err != nil {
			return err
		}
	}
	if result.Table == nil || !hasKey(result.Table.KeySchema, identifierCol, types.KeyTypeHash) || !hasKey(result.Table.KeySchema, namespaceCol, types.KeyTypeRange) {
		return errors.New("DynamoDB catalog table has an incompatible key schema")
	}
	for _, name := range []string{identifierCol, namespaceCol} {
		valid := false
		for _, attribute := range result.Table.AttributeDefinitions {
			if aws.ToString(attribute.AttributeName) == name && attribute.AttributeType == types.ScalarAttributeTypeS {
				valid = true
			}
		}
		if !valid {
			return fmt.Errorf("DynamoDB catalog key %s must have string type", name)
		}
	}
	for _, index := range result.Table.GlobalSecondaryIndexes {
		if index.IndexStatus == types.IndexStatusActive && aws.ToString(index.IndexName) == namespaceIndex && hasKey(index.KeySchema, namespaceCol, types.KeyTypeHash) && hasKey(index.KeySchema, identifierCol, types.KeyTypeRange) {
			return nil
		}
	}

	return errors.New("DynamoDB catalog table is missing the namespace-identifier index")
}

func hasKey(keys []types.KeySchemaElement, name string, kind types.KeyType) bool {
	for _, key := range keys {
		if aws.ToString(key.AttributeName) == name && key.KeyType == kind {
			return true
		}
	}

	return false
}

func stringAttr(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }
func attrString(value types.AttributeValue) string {
	if s, ok := value.(*types.AttributeValueMemberS); ok {
		return s.Value
	}

	return ""
}

func namespaceKey(ns table.Identifier) item {
	return item{identifierCol: stringAttr(namespaceIdentifier), namespaceCol: stringAttr(strings.Join(ns, "."))}
}

func tableKey(ident table.Identifier) item {
	return item{identifierCol: stringAttr(strings.Join(ident, ".")), namespaceCol: stringAttr(strings.Join(catalog.NamespaceFromIdent(ident), "."))}
}

func validateNamespace(ns table.Identifier, allowEmpty bool) error {
	if len(ns) == 0 && !allowEmpty {
		return catalog.ErrNoSuchNamespace
	}
	for _, part := range ns {
		if part == "" || strings.Contains(part, ".") {
			return fmt.Errorf("%w: namespace components must be nonempty and contain no dots", catalog.ErrInvalidIdentifier)
		}
	}

	return nil
}

func validateIdentifier(ident table.Identifier) error {
	if err := catalog.ValidateTableIdentifier(ident); err != nil {
		return err
	}
	if err := validateNamespace(catalog.NamespaceFromIdent(ident), false); err != nil {
		return err
	}
	if strings.Contains(catalog.ObjectNameFromIdent(ident), ".") {
		return catalog.ErrInvalidIdentifier
	}

	return nil
}

func (c *Catalog) get(ctx context.Context, key item) (item, error) {
	output, err := c.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(c.tableName), Key: key, ConsistentRead: aws.Bool(true)})
	if err != nil {
		return nil, err
	}

	return output.Item, nil
}

func setEntryMetadata(entry item, fresh bool) {
	now := &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().UnixMilli(), 10)}
	if fresh {
		entry["created_at"] = now
	}
	entry["updated_at"] = now
	entry[versionCol] = stringAttr(uuid.NewString())
}

func properties(entry item) iceberg.Properties {
	props := iceberg.Properties{}
	for key, value := range entry {
		if property, ok := strings.CutPrefix(key, "p."); ok {
			props[property] = attrString(value)
		}
	}

	return props
}

func (c *Catalog) fileSystem(props iceberg.Properties, location string) table.FSysF {
	return func(ctx context.Context) (icebergio.IO, error) {
		return icebergio.LoadFS(utils.WithAWSConfig(ctx, &c.awsConfig), props, location)
	}
}

func (c *Catalog) transaction(ctx context.Context, writes []types.TransactWriteItem) error {
	_, err := c.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: writes, ClientRequestToken: aws.String(uuid.NewString()),
	})

	return err
}

func conditionFailed(err error) bool {
	if _, failed := errors.AsType[*types.ConditionalCheckFailedException](err); failed {
		return true
	}
	if canceled, ok := errors.AsType[*types.TransactionCanceledException](err); ok {
		for _, reason := range canceled.CancellationReasons {
			if aws.ToString(reason.Code) == "ConditionalCheckFailed" {
				return true
			}
		}
	}

	return false
}

func transactionConflict(err error) bool {
	if _, conflict := errors.AsType[*types.TransactionConflictException](err); conflict {
		return true
	}
	if canceled, ok := errors.AsType[*types.TransactionCanceledException](err); ok {
		for _, reason := range canceled.CancellationReasons {
			if aws.ToString(reason.Code) == "TransactionConflict" {
				return true
			}
		}
	}

	return false
}
