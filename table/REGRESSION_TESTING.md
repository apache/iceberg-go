# Regression Testing for Apache Iceberg Go Table Package

This document describes the regression testing framework implemented for the Apache Iceberg Go table package to prevent regressions in core functionality.

## Overview

Regression tests are designed to ensure that existing functionality continues to work correctly when new changes are made to the codebase. These tests focus on verifying that critical features maintain their expected behavior across code modifications.

## Test Structure

### CoreRegressionTestSuite

The main regression test suite (`regression_core_test.go`) covers fundamental table operations and ensures they remain stable:

- **Table Creation & Consistency** - Verifies table creation with various schema types
- **Schema Management** - Tests field ID uniqueness, type validation, and schema preservation  
- **Partition Specifications** - Validates partition spec consistency and transform functionality
- **Data Integration** - Ensures data read/write operations maintain integrity
- **Transaction Operations** - Tests basic transaction lifecycle and error handling
- **Case Sensitivity** - Verifies proper handling of case-sensitive field names
- **Property Management** - Tests table property preservation and updates
- **Error Handling** - Ensures consistent error handling across operations
- **Metadata Operations** - Validates metadata access and schema equality

## Key Test Categories

### 1. Schema Validation Tests

```go
TestSchemaFieldIDUniqueness()    // Ensures field IDs remain unique
TestSchemaTypeValidation()       // Validates all primitive types work correctly  
TestCaseSensitivityHandling()    // Tests case-sensitive field operations
```

### 2. Table Operations Tests

```go
TestTableCreationConsistency()   // Verifies consistent table creation
TestBasicTableOperations()       // Tests core table functionality
TestDataIntegration()           // Validates end-to-end data workflows
```

### 3. Partition Management Tests

```go
TestPartitionSpecConsistency()   // Tests partition spec preservation
TestTransformValidation()        // Validates partition transforms work correctly
```

### 4. Error Handling Tests

```go
TestErrorHandlingConsistency()   // Ensures proper error handling behavior
```

## Test Design Principles

### 1. **Focus on Core Functionality**
Tests target the most critical features that users depend on daily:
- Table creation and metadata access
- Schema field management and type handling
- Data read/write operations
- Transaction lifecycle management

### 2. **Use Real APIs**
Tests use the actual public APIs rather than mocking, ensuring they catch real-world issues:
```go
// Real table creation
tbl, err := s.cat.CreateTable(s.ctx, identifier, schema)

// Real data operations  
tx := tbl.NewTransaction()
err = tx.AddFiles(s.ctx, files, nil, false)
```

### 3. **Comprehensive Edge Cases**
Tests cover edge cases that could break in subtle ways:
- Empty tables and zero-data scenarios
- Complex schema types (structs, lists, maps)
- Various partition transforms and field arrangements
- Case sensitivity edge cases

### 4. **Clear Assertions**
Each test makes specific, meaningful assertions:
```go
// Check for specific expected values
s.Require().Equal("append", string(snapshot.Summary.Operation))

// Verify data integrity
s.Require().True(testData.Schema().Equal(result.Schema()))
```

## Benefits for Iceberg Development

### 1. **Prevent Breaking Changes**
- Catches accidental API changes that break existing functionality
- Ensures backward compatibility is maintained
- Validates that refactoring doesn't change behavior

### 2. **Enable Confident Refactoring** 
- Developers can refactor internal code knowing the API behavior is verified
- Supports large-scale code improvements and optimizations
- Provides safety net for dependency updates

### 3. **Document Expected Behavior**
- Tests serve as executable documentation of how features should work
- Captures important edge cases and expected error conditions
- Provides examples of proper API usage

### 4. **Early Bug Detection**
- Catches issues immediately when they're introduced
- Prevents bugs from reaching production environments
- Reduces debugging time by pinpointing when regressions occur

## Running the Tests

### Run All Regression Tests
```bash
go test -run "TestCoreRegression" -v
```

### Run Specific Test Category
```bash
go test -run "TestCoreRegression/TestDataIntegration" -v
```

### Integration with CI/CD
The regression tests integrate seamlessly with existing CI pipelines:
```bash
# Run as part of standard test suite
go test ./table/... -v

# Run only regression tests for quick validation
go test -run ".*Regression.*" ./table/...
```

## Test Maintenance

### Adding New Regression Tests

When adding new functionality to the table package:

1. **Identify Critical Paths**: Determine which new features are critical for users
2. **Create Focused Tests**: Write tests that verify the core behavior
3. **Use Descriptive Names**: Name tests clearly to indicate what they validate
4. **Include Edge Cases**: Test boundary conditions and error scenarios

Example test structure:
```go
func (s *CoreRegressionTestSuite) TestNewFeatureBehavior() {
    // Setup test data
    
    // Execute the feature
    
    // Verify expected behavior
    s.Require().Equal(expected, actual)
    
    // Test edge cases
    
    // Verify error handling
}
```

### Updating Existing Tests

When modifying existing functionality:

1. **Update Tests First**: Modify tests to reflect intended new behavior
2. **Ensure Backward Compatibility**: Verify changes don't break existing users
3. **Add Migration Tests**: Test upgrades from old behavior to new behavior
4. **Document Breaking Changes**: Clearly note any intentional breaking changes

## Test Coverage Areas

### Currently Covered
- ✅ Table creation and metadata access
- ✅ Schema field management and validation
- ✅ Basic transaction operations
- ✅ Partition specification handling
- ✅ Data read/write operations
- ✅ Error handling consistency
- ✅ Case sensitivity behavior
- ✅ Property management

### Future Expansion Areas
- 🔄 Complex transaction scenarios (concurrent modifications, rollbacks)
- 🔄 Schema evolution operations (field addition, removal, renaming)
- 🔄 Advanced partition operations (spec evolution, pruning optimization)
- 🔄 Performance regression detection
- 🔄 Integration with external catalogs
- 🔄 Data format compatibility (Arrow, Parquet integration)

## Best Practices

### 1. **Keep Tests Independent**
Each test should be able to run independently without relying on other tests:
```go
func (s *CoreRegressionTestSuite) SetupTest() {
    // Fresh setup for each test
    s.location = filepath.ToSlash(s.T().TempDir())
    s.cat, _ = catalog.Load(...)
}
```

### 2. **Use Descriptive Test Names**
Test names should clearly indicate what functionality they verify:
```go
TestSchemaFieldIDUniqueness()        // Clear purpose
TestPartitionSpecConsistency()       // Specific feature  
TestErrorHandlingConsistency()       // Behavior category
```

### 3. **Fail Fast with Clear Messages**
Provide helpful error messages for debugging:
```go
s.Require().True(found, "Field %s not found", fieldName)
s.Require().Equal(expected, actual, "Property %s value mismatch", key)
```

### 4. **Test Real Scenarios**
Use realistic data and operations that mirror actual usage:
```go
// Real Arrow table creation
testData := s.createTestData(10)
defer testData.Release()

// Real file operations
filePath := fmt.Sprintf("%s/data/test.parquet", s.location)
s.writeParquetFile(fs.(iceio.WriteFileIO), filePath, testData)
```

## Integration with Development Workflow

### Pre-commit Testing
```bash
# Run regression tests before committing
go test -run "TestCoreRegression" ./table/... --short
```

### Pull Request Validation
```bash
# Full regression test suite for PR validation
go test ./table/... -v -timeout=10m
```

### Release Testing
```bash
# Comprehensive test run for releases
go test ./... -v -race -timeout=30m
```

## Troubleshooting

### Common Issues

**Test Timeouts**: Some tests create real tables and data files, which can be slow
- Solution: Use `-timeout` flag or run with `-short` for faster execution

**Flaky Tests**: Occasional failures due to timing or resource constraints  
- Solution: Tests use isolated temporary directories and cleanup properly

**Missing Dependencies**: Tests require Arrow and Parquet libraries
- Solution: Run `go mod download` to ensure all dependencies are available

### Debugging Failed Tests

When regression tests fail:

1. **Identify the Failing Assertion**: Look at the specific requirement that failed
2. **Check Recent Changes**: Review what code changes might have affected the behavior  
3. **Run Individual Tests**: Isolate the failing test to understand the specific issue
4. **Verify Test Data**: Ensure test setup is creating the expected initial conditions
5. **Check API Changes**: Verify if the failure indicates an intentional API change

## Conclusion

The regression testing framework provides confidence that core Iceberg table functionality remains stable as the codebase evolves. By focusing on real-world usage patterns and critical functionality, these tests serve as both a safety net for developers and documentation of expected behavior.

The tests are designed to be maintainable, fast, and reliable while providing comprehensive coverage of the most important table package features. They support the long-term stability and reliability of the Apache Iceberg Go implementation. 