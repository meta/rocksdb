# Customizable and Configurable Framework

**Files:** `include/rocksdb/configurable.h`, `include/rocksdb/customizable.h`, `options/configurable.cc`, `options/customizable.cc`, `options/configurable_helper.h`, `include/rocksdb/utilities/options_type.h`, `include/rocksdb/utilities/object_registry.h`

## Overview

The Configurable and Customizable classes form a framework for creating RocksDB components that can be configured from strings, serialized to strings, compared for equivalence, and instantiated from a registry. This framework powers the options system, enabling polymorphic objects like table factories, compaction filters, merge operators, and caches to participate in the unified configuration infrastructure.

## Class Hierarchy

`Configurable` is the base class that provides configuration, serialization, and comparison capabilities. `Customizable` extends `Configurable` with naming, identity, and factory-based creation.

```
Configurable
  |
  +-- Customizable
        |
        +-- TableFactory (BlockBasedTableFactory, PlainTableFactory, ...)
        +-- Cache (LRUCache, HyperClockCache, ...)
        +-- FilterPolicy (BloomFilterPolicy, RibbonFilterPolicy, ...)
        +-- MergeOperator
        +-- CompactionFilter
        +-- SliceTransform
        +-- FileSystem
        +-- Env
        +-- ... (many more)
```

## Configurable Base Class

`Configurable` (defined in `include/rocksdb/configurable.h`) provides the core infrastructure for option management.

### Registration

Derived classes register their options in their constructor by calling `RegisterOptions()`. This method takes:

- A name string identifying the option set
- A pointer to the options struct
- A pointer to an `std::unordered_map<std::string, OptionTypeInfo>` describing each field

The registration stores the offset between the `Configurable` object and the options pointer, making registrations portable across instances of the same type.

### Configuration

Configuration methods accept options in multiple formats:

| Method | Input Format | Description |
|--------|-------------|-------------|
| `ConfigureFromMap()` | `unordered_map<string, string>` | Configure from name-value map |
| `ConfigureFromString()` | `string` | Configure from semicolon-delimited string |
| `ConfigureOption()` | `string name, string value` | Configure a single option |

The configuration flow through `ConfigureFromMap()`:

Step 1: Save current options state (for rollback on failure)

Step 2: Disable `invoke_prepare_options` temporarily during individual option processing

Step 3: Call `ConfigurableHelper::ConfigureOptions()` which iterates registered option maps, matching input names to `OptionTypeInfo` entries and calling `Parse()` on each match

Step 4: If `invoke_prepare_options` is true, call `PrepareOptions()` to validate and initialize

Step 5: On failure, attempt to restore the previous options state via `ConfigureFromString()` with the saved state

The iterative matching in Step 3 uses multiple passes: options that depend on other options (e.g., a Customizable that references another option) may fail on the first pass but succeed after their dependencies are configured. The loop continues as long as at least one option was successfully processed per pass.

### Serialization

Serialization converts the registered options back to string form:

- `GetOptionString()` -- returns semicolon-delimited `name=value` pairs
- `ToString()` -- returns `{name=value;...}` (with braces for nested objects)
- `GetOption()` -- retrieves a single option's value by name

Serialization iterates all registered `OptionTypeInfo` entries, calling `Serialize()` on each. Options marked with `OptionTypeFlags::kDontSerialize` or `OptionVerificationType::kDeprecated` are skipped.

### Comparison

`AreEquivalent()` compares two `Configurable` objects field by field using their registered `OptionTypeInfo` maps. The comparison respects the `SanityLevel` setting from `ConfigOptions`:

- `kSanityLevelNone`: Always returns equal
- `kSanityLevelLooselyCompatible`: Only compares options with loose-level sanity marking
- `kSanityLevelExactMatch`: Compares every option

For options that cannot be compared by value (e.g., pointer types like `Comparator*`), `OptionsAreEqual()` falls back to `AreEqualByName()`, which compares the string serialization of both values.

### PrepareOptions and ValidateOptions

`PrepareOptions()` is called after configuration to perform initialization and validation. It recursively prepares all nested `Configurable` and `Customizable` sub-options. Implementations should be idempotent since `PrepareOptions()` may be called multiple times.

`ValidateOptions()` checks whether the configured options are compatible with the given `DBOptions` and `ColumnFamilyOptions`. For example, a table factory might validate that its options are consistent with the environment's capabilities.

## Customizable Class

`Customizable` (defined in `include/rocksdb/customizable.h`) extends `Configurable` with:

### Naming and Identity

- `Name()` -- returns the class name (e.g., `"BlockBasedTable"`, `"LRUCache"`)
- `GetId()` -- returns an identifier, typically the same as `Name()` but can include configuration (e.g., a URL pattern)
- `NickName()` -- optional short alias (e.g., `"put"` for `PutOperator`)
- `GenerateIndividualId()` -- creates a unique ID of the form `Name@address#pid`
- `IsInstanceOf()` -- checks if the object matches a given name (by `Name()` or `NickName()`)

### CheckedCast

`CheckedCast<T>()` provides safe downcasting by combining `IsInstanceOf()` with `static_cast`. It also traverses the `Inner()` chain for wrapped objects:

```
auto* lru = cache->CheckedCast<LRUCache>();  // returns nullptr if not LRU
```

### Has-A Relationship (Inner)

When a `Customizable` wraps another (e.g., `SharedCache` wrapping `LRUCache`), the wrapper overrides `Inner()` to return the wrapped object. This enables `CheckedCast` and `GetOptionsPtr` to traverse the wrapping chain.

### Factory Creation via GetOptionsMap

`Customizable::GetOptionsMap()` parses a configuration string to determine which Customizable to create and how to configure it. The ID resolution logic:

1. If the value is a simple name (no `=` or `;`), use it as the ID
2. If the value contains `id=value` pairs, extract the `id` field
3. If the value matches the current object's ID, merge the current object's options with the new ones (allowing partial updates)
4. Empty value or `"nullptr"` creates a null object

This method is used by `CreateFromString()` factory methods on various base classes (e.g., `TableFactory::CreateFromString()`, `Cache::CreateFromString()`).

### Serialization

`Customizable::SerializeOptions()` overrides the base behavior:

- **Shallow mode**: Returns just the ID (e.g., `"BlockBasedTable"`)
- **Deep mode**: Returns `id=BlockBasedTable;option1=value1;option2=value2;...`

This enables compact representation in simple cases while supporting full round-trip serialization.

### Equivalence

`Customizable::AreEquivalent()` first compares IDs. If the IDs differ, the objects are not equivalent. If IDs match and the sanity level requires detailed comparison, it delegates to `Configurable::AreEquivalent()` for field-by-field comparison.

## Object Registry

The `ObjectRegistry` (defined in `include/rocksdb/utilities/object_registry.h`) is the factory mechanism for creating `Customizable` objects from string identifiers.

### ObjectLibrary

An `ObjectLibrary` contains a set of factory entries, each mapping a pattern to a factory function. Patterns are defined using the `PatternEntry` class which supports:

- Exact name matching: `PatternEntry("LRUCache")`
- Name with separator: `PatternEntry("bloomfilter").AddSeparator(":")`
- Name with number: `PatternEntry("id").AddNumber(":")`
- Alternative names: `PatternEntry("LRUCache").AnotherName("lru_cache")`
- Individual ID matching: `PatternEntry::AsIndividualId("Name")` matches `Name@address#pid`

### Registry Hierarchy

`ObjectRegistry` supports a parent-child relationship. When an object is not found in the current registry, the parent registry is consulted. This enables extension: applications can create a child registry with custom factories while inheriting all built-in factories from the default registry.

### CreateFromString Pattern

Each Customizable base class provides a static `CreateFromString()` method that:

Step 1: Calls `Customizable::GetOptionsMap()` to split the input into ID and options

Step 2: If the existing object matches the ID, reconfigures it with the new options

Step 3: Otherwise, looks up the ID in the `ObjectRegistry` to create a new object

Step 4: Calls `Customizable::ConfigureNewObject()` to configure and prepare the new object

This pattern enables both creation of new objects and reconfiguration of existing ones from string values.

## ConfigurableHelper

`ConfigurableHelper` (defined in `options/configurable_helper.h`) provides the static implementation methods that power the `Configurable` class:

- `ConfigureOptions()` -- iterates registered option maps to configure from a map
- `ConfigureSomeOptions()` -- processes a subset of options from a single type map
- `ConfigureSingleOption()` -- configures one option by name
- `ConfigureCustomizableOption()` -- handles the complex logic of configuring Customizable-typed options, including mutable vs immutable checks and object replacement
- `SerializeOptions()` -- serializes all registered options to a string
- `AreEquivalent()` -- compares two Configurables field by field
- `FindOption()` -- locates an `OptionTypeInfo` entry by name across all registered maps

### Customizable Option Configuration

`ConfigureCustomizableOption()` handles the most complex case: configuring an option that points to a Customizable object. The logic differs based on whether `mutable_options_only` is set:

**When processing all options** (mutable_options_only=false): If the option name matches the top-level name (e.g., `table_factory=...`), the entire Customizable may be replaced. If the name is a sub-field (e.g., `BlockBasedTable.block_size=4096`), the existing Customizable is reconfigured.

**When processing mutable options only** (mutable_options_only=true): The Customizable object itself cannot be replaced. Only its mutable sub-options can be updated. Attempting to change the ID returns `InvalidArgument`.

## Integration with Options System

The Configurable/Customizable framework integrates with the broader options system through adapter functions in `options/options_helper.h`:

- `DBOptionsAsConfigurable()` -- wraps `DBOptions` in a `Configurable` that has registered all DB option type info maps
- `CFOptionsAsConfigurable()` -- wraps `ColumnFamilyOptions` in a `Configurable` with CF option type info maps

These adapters enable the generic parsing, serialization, and comparison infrastructure to work with the concrete options structs. The `GetDBOptionsFromMap()`, `GetStringFromDBOptions()`, and similar functions all use these adapters internally.
