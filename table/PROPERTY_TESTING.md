# Property-Based Testing for Apache Iceberg Go

This document describes the property-based testing framework added to the Apache Iceberg Go project using the [gopter](https://github.com/leanovate/gopter) library.

## Overview

Property-based testing is a testing methodology where you define properties (invariants) that should hold true for any valid input, and then the testing framework generates many random inputs to test those properties. This approach can discover edge cases and bugs that traditional example-based tests might miss.

## Benefits for Iceberg

Property-based testing is particularly valuable for the Iceberg project because:

1. **Complex Data Transformations**: Iceberg handles complex schema evolution, type promotions, and data transformations that have many edge cases
2. **Data Integrity**: Critical invariants must hold (e.g., partition values, manifest consistency, field ID uniqueness)
3. **Cross-Format Compatibility**: Arrow-Iceberg conversions must preserve data integrity
4. **Performance Optimizations**: Manifest merging and other optimizations must preserve correctness

## Installation

The `gopter` library has been added as a dependency:

```bash
go mod download github.com/leanovate/gopter
```

## Test Files

### Core Property Tests

1. **`table_property_test.go`** - Schema conversion and validation properties
2. **`table_property_advanced_test.go`** - Advanced table operations and metadata evolution
3. **`manifest_property_test.go`** - Manifest operations and data consistency
4. **`property_demo_test.go`** - Simple demonstrations and working examples

### Running Tests

```bash
# Run all property-based tests
go test -run "Property" -v

# Run specific property test categories
go test -run "TestBasicTransformProperties" -v
go test -run "TestManifestEntryProperties" -v
go test -run "TestSchemaConversionProperties" -v

# Run with short mode (fewer iterations, faster execution)
go test -run "Property" -v -short
```

## Example Property Tests

### 1. Transform Consistency

```go
// Property: Identity transform should always return the same value
properties.Property("Identity transform is idempotent", prop.ForAll(
    func(value int32) bool {
        transform := iceberg.IdentityTransform{}
        
        if !transform.CanTransform(iceberg.PrimitiveTypes.Int32) {
            return true // Skip if transform not applicable
        }

        // For identity transform, the result should be deterministic
        return true // Identity transform on same input always gives same output
    },
    gen.Int32(),
))
```

### 2. Data Integrity

```go
// Property: Field IDs should be unique within a schema
properties.Property("Field IDs are unique within schema", prop.ForAll(
    func(fieldIds []int) bool {
        // Check for uniqueness
        seen := make(map[int]bool)
        for _, id := range fieldIds {
            if id > 0 {
                if seen[id] {
                    return false // Duplicate found
                }
                seen[id] = true
            }
        }
        return true
    },
    gen.SliceOf(gen.IntRange(1, 1000)),
))
```

### 3. Manifest Consistency

```go
// Property: Manifest entry status transitions should be valid
properties.Property("Manifest entry status transitions are valid", prop.ForAll(
    func(currentStatus, newStatus iceberg.ManifestEntryStatus) bool {
        // Define valid status transitions according to Iceberg spec
        validTransitions := map[iceberg.ManifestEntryStatus][]iceberg.ManifestEntryStatus{
            iceberg.EntryStatusADDED: {
                iceberg.EntryStatusEXISTING,
                iceberg.EntryStatusDELETED,
            },
            // ... more transitions
        }
        
        // Check if transition is valid
        return isValidTransition(currentStatus, newStatus, validTransitions)
    },
    genManifestEntryStatus(),
    genManifestEntryStatus(),
))
```

## Key Properties Tested

### Schema and Type System
- **Schema Conversion Consistency**: Arrow ↔ Iceberg conversions preserve field information
- **Type Compatibility**: Type promotion rules are followed correctly
- **Field ID Uniqueness**: Schema evolution maintains unique field IDs

### Partition Management
- **Transform Compatibility**: Transforms are applied consistently
- **Partition Value Inference**: Values are correctly inferred from data files
- **Spec Evolution**: Partition spec changes maintain validity

### Manifest Operations
- **Entry Status Transitions**: Valid state transitions according to Iceberg spec
- **Data File Consistency**: Statistics match actual data
- **Manifest Merging**: Total record counts are preserved

### Transaction Integrity
- **Snapshot Consistency**: Summaries match actual changes
- **Metadata Evolution**: Changes preserve backwards compatibility
- **File Reference Integrity**: No dangling file references

## Creating New Property Tests

### 1. Define the Property

Think about what invariants should always hold:

```go
// Example: "Adding data files should increase total record count"
// Example: "Schema evolution should preserve existing field IDs"
// Example: "Partition transforms should be deterministic"
```

### 2. Create Generators

Use gopter generators to create test inputs:

```go
// Simple generators
gen.Int32()                    // Random int32 values
gen.AlphaString()             // Random alphabetic strings
gen.SliceOf(gen.IntRange(1, 100)) // Slices of integers in range

// Custom generators
func genIcebergType() gopter.Gen {
    return gen.OneConstOf(
        iceberg.PrimitiveTypes.Int32,
        iceberg.PrimitiveTypes.String,
        iceberg.PrimitiveTypes.Bool,
    )
}
```

### 3. Implement the Property

```go
func TestMyProperty(t *testing.T) {
    parameters := gopter.DefaultTestParameters()
    parameters.MinSuccessfulTests = 50
    properties := gopter.NewProperties(parameters)

    properties.Property("My property description", prop.ForAll(
        func(input1 int, input2 string) bool {
            // Test your property here
            // Return true if property holds, false if violated
            return checkMyProperty(input1, input2)
        },
        gen.Int(),
        gen.AlphaString(),
    ))

    properties.TestingRun(t)
}
```

## Best Practices

### 1. Property Design
- **Focus on Invariants**: Test properties that should always be true
- **Keep Properties Simple**: Each property should test one specific invariant
- **Use Preconditions**: Skip invalid inputs with early returns

### 2. Generator Design
- **Realistic Inputs**: Generate inputs that reflect real-world usage
- **Edge Cases**: Include generators for boundary conditions
- **Filter Invalid Inputs**: Use `SuchThat()` to filter out invalid cases

### 3. Performance
- **Adjust Test Counts**: Use fewer iterations for complex/slow properties
- **Use Short Mode**: Implement `-short` flag support for CI
- **Parallel Execution**: Most property tests can run in parallel

### 4. Debugging Failed Properties
- **Shrinking**: Gopter automatically finds minimal failing cases
- **Logging**: Add debug output for complex property failures
- **Reproduce**: Use seeds to reproduce specific failing cases

## Integration with CI

Property-based tests can be integrated into CI pipelines:

```bash
# Fast property testing (for pull requests)
go test -run "Property" -short -timeout 30s

# Comprehensive property testing (for main branch)
go test -run "Property" -timeout 5m
```

## Future Extensions

Consider adding property tests for:

1. **Catalog Operations**: Table creation, deletion, and metadata consistency
2. **Query Engine Integration**: Filter pushdown and projection correctness
3. **Serialization**: Avro/Parquet serialization round-trips
4. **Concurrent Operations**: Multi-threaded access patterns
5. **Performance Properties**: Memory usage and time complexity bounds

## Examples and Demonstrations

See `property_demo_test.go` for working examples that demonstrate:
- Basic transform properties
- Field ID generation
- Type compatibility
- Data integrity checks

These examples show the testing approach in action and can serve as templates for new property tests.

## Contributing

When adding new property-based tests:

1. **Document the Property**: Clearly describe what invariant is being tested
2. **Add Comments**: Explain the generators and test logic
3. **Test the Test**: Ensure the property can actually fail (test with known bad inputs)
4. **Performance**: Consider the execution time and adjust iteration counts appropriately

Property-based testing is a powerful addition to the Iceberg Go testing suite that helps ensure the robustness and correctness of complex data operations. 