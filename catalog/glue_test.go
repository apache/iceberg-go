package catalog

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/stretchr/testify/require"
)

func TestGlueGetTableIntegration(t *testing.T) {
	if os.Getenv("TEST_DATABASE_NAME") == "" {
		t.Skip()
	}
	if os.Getenv("TEST_TABLE_NAME") == "" {
		t.Skip()
	}
	assert := require.New(t)

	awscfg, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(aws.LogRequest|aws.LogResponse))
	assert.NoError(err)

	catalog := NewGlueCatalog(awscfg)

	table, err := catalog.GetTable(context.TODO(), GlueTableIdentifier(os.Getenv("TEST_DATABASE_NAME"), os.Getenv("TEST_TABLE_NAME")))
	assert.NoError(err)
	assert.Equal([]string{os.Getenv("TEST_TABLE_NAME")}, table.Identifier())
}

func TestGlueListTableIntegration(t *testing.T) {
	if os.Getenv("TEST_DATABASE_NAME") == "" {
		t.Skip()
	}

	assert := require.New(t)

	awscfg, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(aws.LogRequest|aws.LogResponse))
	assert.NoError(err)

	catalog := NewGlueCatalog(awscfg)

	tables, err := catalog.ListTables(context.TODO(), GlueDatabaseIdentifier(os.Getenv("TEST_DATABASE_NAME")))
	assert.NoError(err)
	assert.Equal([]string{os.Getenv("TEST_TABLE_NAME")}, tables[1].Identifier())
}
