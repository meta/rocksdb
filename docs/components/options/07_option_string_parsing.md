# Option String Parsing

**Files:** `include/rocksdb/convenience.h`, `options/options_helper.h`, `options/options_helper.cc`, `include/rocksdb/utilities/options_type.h`

## Overview

RocksDB provides a complete framework for converting options between their native C++ representation and human-readable strings. This enables configuration via text files, command-line arguments, dynamic option updates, and debugging. The system supports three input formats: option strings, option maps, and option files (covered in the next chapter).

## Option String Format

An option string is a semicolon-delimited list of `name=value` pairs. The format supports nested options for complex types using curly braces.

**Basic format:**
```
name1=value1;name2=value2;name3=value3
```

**Nested format (for struct or Customizable options):**
```
write_buffer_size=1024;block_based_table_factory={block_size=4k;filter_policy=bloomfilter:10:false}
```

The parsing is implemented by `StringToMap()` (declared in `include/rocksdb/convenience.h`, defined in `options/options_helper.cc`), which converts the string into an `std::unordered_map<std::string, std::string>`. The function handles nested braces by tracking brace depth: when a `{` is encountered, it scans forward counting nested `{`/`}` until the matching `}` is found, preserving the entire nested value including internal semicolons.

## Value Type Parsing Rules

The parsing rules for converting string values to their native types are documented in `include/rocksdb/convenience.h` and implemented in the `OptionTypeInfo` framework.

| Type | Format | Examples |
|------|--------|---------|
| Boolean | `"true"`, `"1"`, `"false"`, `"0"` | `"true"`, `"0"` |
| Integer | Decimal with optional size suffix | `"1024"`, `"64K"`, `"256M"`, `"4G"` |
| Double | Standard floating-point notation | `"0.1"`, `"3.14"` |
| String | Raw string (no trimming or truncation) | `"my_path"` |
| Enum | Enum constant name | `"kSnappyCompression"`, `"kCompactionStyleLevel"` |
| Vector | Colon-separated values | `"kNoCompression:kSnappyCompression:kZSTD"` |
| Struct | Semicolon-separated fields in braces, or dotted field names | `"{window_bits=4;level=5}"` or `"compression_opts.level=5"` |
| Customizable | Name alone, or `{id=Name;opt1=val1;...}` | `"bloomfilter:10:false"` |

**Integer size suffixes** are powers of 2: `K`/`k` = 2^10, `M`/`m` = 2^20, `G`/`g` = 2^30, `T`/`t` = 2^40 (unsigned 64-bit only).

**Compression options** support a compact colon-delimited format: `"window_bits:level:strategy:max_dict_bytes"`. For example, `"4:5:6:7"` sets `window_bits=4`, `level=5`, `strategy=6`, `max_dict_bytes=7`.

## Option Map

An option map is an `std::unordered_map<std::string, std::string>` that maps option names to string values. Option maps are the intermediate representation used by all parsing paths -- string parsing first converts to a map via `StringToMap()`, then the map is processed.

The public API functions for map-based parsing are declared in `include/rocksdb/convenience.h`:

- `GetColumnFamilyOptionsFromMap()` -- parses a map into `ColumnFamilyOptions`
- `GetDBOptionsFromMap()` -- parses a map into `DBOptions`
- `GetBlockBasedTableOptionsFromMap()` -- parses a map into `BlockBasedTableOptions`
- `GetPlainTableOptionsFromMap()` -- parses a map into `PlainTableOptions`

## String-Based Parsing API

The string-based convenience functions first call `StringToMap()` then delegate to the map-based functions:

- `GetColumnFamilyOptionsFromString()` -- parses a string into `ColumnFamilyOptions`
- `GetDBOptionsFromString()` -- parses a string into `DBOptions`
- `GetOptionsFromString()` -- parses a string into `Options` (combined DB + CF options)
- `GetBlockBasedTableOptionsFromString()` -- parses a string into `BlockBasedTableOptions`

All parsing functions take a base options object and apply the parsed values on top of it. Options not mentioned in the string retain their values from the base options.

## Serialization API

The inverse path converts options structs to their string representation:

- `GetStringFromDBOptions()` -- serializes `DBOptions` to string
- `GetStringFromColumnFamilyOptions()` -- serializes `ColumnFamilyOptions` to string

These functions wrap the options in a `Configurable` adapter (via `DBOptionsAsConfigurable()` or `CFOptionsAsConfigurable()` in `options/options_helper.h`) and call `GetOptionString()` on the adapter. The result is a delimiter-separated sequence of `name=value` pairs.

The delimiter defaults to `";"` for inline strings and `"\n  "` for OPTIONS files, controlled by `ConfigOptions::delimiter`.

## ConfigOptions

`ConfigOptions` (defined in `include/rocksdb/convenience.h`) controls all parsing, serialization, and comparison behavior. Key fields:

| Field | Default | Purpose |
|-------|---------|---------|
| `ignore_unknown_options` | `false` | When true, unknown option names are silently ignored |
| `ignore_unsupported_options` | `true` | When true, unsupported but recognized options are ignored |
| `input_strings_escaped` | `true` | Whether input strings use backslash escaping |
| `invoke_prepare_options` | `true` | Whether to call `PrepareOptions()` after configuration |
| `mutable_options_only` | `false` | When true, only mutable options are accepted |
| `delimiter` | `";"` | Separator between serialized options |
| `depth` | `kDepthDefault` | Controls traversal of nested options |
| `sanity_level` | `kSanityLevelExactMatch` | Controls strictness of option comparison |
| `env` | `Env::Default()` | Environment for file operations |
| `registry` | New `ObjectRegistry` | Registry for creating Customizable objects |

Important: When `ignore_unknown_options` is `true` but the OPTIONS file was generated by the same or earlier RocksDB version, unknown options are treated as corruption rather than being ignored. This check happens in `RocksDBOptionsParser::Parse()` by comparing the file's `rocksdb_version` against the current binary version.

## Parsing Workflow

The end-to-end parsing flow for `GetColumnFamilyOptionsFromString()`:

1. `StringToMap()` converts the input string to an option map
2. `GetColumnFamilyOptionsFromMap()` copies the base options, creates a `Configurable` wrapper via `CFOptionsAsConfigurable()`
3. `ConfigureFromMap()` on the `Configurable` wrapper iterates registered `OptionTypeInfo` entries
4. For each option name found in the map, `OptionTypeInfo::Parse()` converts the string value to the native type and writes it to the correct offset in the options struct
5. If `invoke_prepare_options` is true, `PrepareOptions()` is called to validate and initialize
6. The configured options are extracted from the `Configurable` wrapper back into the output `ColumnFamilyOptions`

If any step fails, the output options are reset to the base options. Errors are translated to `Status::InvalidArgument` for consistency.

## OptionTypeInfo Framework

The `OptionTypeInfo` class (defined in `include/rocksdb/utilities/options_type.h`) is the metadata backbone of the options system. Each option is registered with an `OptionTypeInfo` entry that describes:

- **Type**: The `OptionType` enum (e.g., `kBoolean`, `kSizeT`, `kCompressionType`, `kConfigurable`)
- **Offset**: Byte offset of the field within the options struct
- **Verification type**: How the option is compared (`kNormal`, `kByName`, `kDeprecated`, `kAlias`)
- **Flags**: Behavioral modifiers (`kMutable`, `kShared`, `kUnique`, `kDontSerialize`, `kDontPrepare`)
- **Parse/Serialize/Equals functions**: Optional custom functions for non-standard types

Options are registered in type info maps defined in `options/db_options.cc` and `options/cf_options.cc`. Each map entry associates a string name with an `OptionTypeInfo` that contains the field offset (computed via `offsetof`) and type metadata.

## Enum String Maps

Enum types use bidirectional string maps for parsing and serialization. These maps are defined in `OptionsHelper` (see `options/options_helper.h` and `options/options_helper.cc`):

| Map | Enum Type |
|-----|-----------|
| `compression_type_string_map` | `CompressionType` |
| `compaction_style_string_map` | `CompactionStyle` |
| `compaction_pri_string_map` | `CompactionPri` |
| `checksum_type_string_map` | `ChecksumType` |
| `temperature_string_map` | `Temperature` |
| `encoding_type_string_map` | `EncodingType` |
| `compaction_stop_style_string_map` | `CompactionStopStyle` |
| `prepopulate_blob_cache_string_map` | `PrepopulateBlobCache` |

Parsing uses `ParseEnum()` (lookup by string key) and serialization uses `SerializeEnum()` (linear scan for matching value). Both are template functions in `include/rocksdb/utilities/options_type.h`.

## Nested and Struct Options

Struct options (e.g., `compression_opts`, `compaction_options_universal`) can be configured in two ways:

1. **Inline format**: `compression_opts={window_bits=4;level=5;strategy=6}`
2. **Dotted format**: `compression_opts.level=5`

The `OptionTypeInfo::ParseStruct()` method handles both cases. When the option name matches the struct name, the value is parsed as a complete inline struct. When the option name has a dotted suffix (e.g., `compression_opts.level`), only the specified field is updated.

## Table Factory Options

Table factory options (e.g., `BlockBasedTableOptions`) are configured as nested Customizable objects:

```
block_based_table_factory={block_size=4096;filter_policy=bloomfilter:10:false}
```

The table factory name (`block_based_table_factory` or `plain_table_factory`) is recognized as a column family option that maps to `table_factory`. The nested value is parsed by the table factory's own `OptionTypeInfo` map.

## Error Handling

All parsing functions return `Status` with specific error codes:

| Status | Meaning |
|--------|---------|
| `OK` | All options parsed successfully |
| `NotFound` | An option name was not recognized |
| `NotSupported` | An option was recognized but its value could not be converted (e.g., missing Customizable factory) |
| `InvalidArgument` | An option value was malformed or out of range |

The public API functions (`GetDBOptionsFromMap`, etc.) translate all non-OK statuses to `InvalidArgument` to provide a consistent error interface. Option names are case-sensitive.
