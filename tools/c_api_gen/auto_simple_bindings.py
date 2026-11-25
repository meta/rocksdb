#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

#!/usr/bin/env python3
"""Auto-discover and generate simple RocksDB C API field bindings.

This script follows a strict policy for the families it manages:

- if a supported simple public C++ field already has matching C bindings,
  leave the existing binding in place
- otherwise auto-generate the matching C binding
- if a managed field is explicitly blocklisted as manual or deferred,
  leave it for explicit C API follow-up
- if a field is in a managed family but is not supported by the simple
  generator and there is no manual C binding, fail regeneration
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from json import JSONDecoder
from pathlib import Path
from typing import Iterable

from generate_c_api import render_decl, render_impl, write_if_changed


ROOT = Path(__file__).resolve().parents[2]
INCLUDE_ROOT = ROOT / "include/rocksdb"
SOURCE_ROOT = ROOT / "db"
# Hand-written source templates for the C API. These are generator inputs (not
# user-includable / not compiled), so they live next to the generator rather
# than in include/rocksdb or db/.
TEMPLATE_ROOT = ROOT / "tools/c_api_gen"
GENERATED_INCLUDE_ROOT = ROOT / "c_api_gen"
GENERATED_SOURCE_ROOT = ROOT / "c_api_gen"
CLANG_FORMAT = shutil.which("clang-format")
BLOCKLIST_PATH = ROOT / "tools/c_api_gen/auto_simple_bindings_blocklist.json"
BLOCKLIST_POLICIES = frozenset({"manual", "deferred"})
# ABI-preserving C type overrides: function name -> pinned C type. See
# abi_type_overrides.json for the rationale (RocksDB's C API must not change the
# signature of an already-shipped function even when the C++ field type maps to
# a different C type).
ABI_TYPE_OVERRIDES_PATH = ROOT / "tools/c_api_gen/abi_type_overrides.json"


def load_abi_type_overrides(path: Path) -> dict[str, str]:
    import json

    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    overrides: dict[str, str] = {}
    for entry in data.get("overrides", []):
        name = entry["function"]
        if name in overrides and overrides[name] != entry["c_type"]:
            raise ValueError(
                f"Conflicting ABI type overrides for {name}: "
                f"{overrides[name]} vs {entry['c_type']}"
            )
        overrides[name] = entry["c_type"]
    return overrides


ABI_TYPE_OVERRIDES = load_abi_type_overrides(ABI_TYPE_OVERRIDES_PATH)


@dataclass(frozen=True)
class FamilyConfig:
    struct_name: str
    header: Path
    c_receiver_type: str
    c_receiver_name: str
    c_prefix: str
    mode: str
    field_expr_format: str = "{receiver_name}->rep.{field_name}"
    enum_types: tuple[str, ...] = ()
    # C integer type used for enum-typed fields. This is intentionally
    # per-family to preserve each family's historical ABI: the option families
    # have always exposed enums as `int`, while the listener/job-info families
    # shipped them as `uint32_t` (e.g. the pre-existing
    # rocksdb_flushjobinfo_flush_reason). Do NOT "unify" these to one type --
    # it would change the signatures of already-shipped functions.
    enum_c_type: str = "int"
    section_name: str | None = None
    setter_name_overrides: tuple[tuple[str, str], ...] = ()
    getter_name_overrides: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class OutputGroup:
    name: str
    header_out: Path
    source_out: Path
    families: tuple[FamilyConfig, ...]


@dataclass(frozen=True)
class BlocklistEntry:
    family_name: str
    field_name: str
    policy: str
    reason: str
    tracking_issue: str | None = None


READ_OPTIONS_GROUP = OutputGroup(
    name="Auto-discovered ReadOptions simple",
    header_out=GENERATED_INCLUDE_ROOT / "c_generated_readoptions_auto.h.inc",
    source_out=GENERATED_SOURCE_ROOT / "c_generated_readoptions_auto.cc.inc",
    families=(
        FamilyConfig(
            struct_name="ReadOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_readoptions_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_readoptions",
            mode="options",
            enum_types=("ReadTier", "Env::IOPriority", "Env::IOActivity"),
            enum_c_type="int",
        ),
    ),
)

OPTION_STRUCTS_GROUP = OutputGroup(
    name="Auto-discovered option structs simple",
    header_out=GENERATED_INCLUDE_ROOT / "c_generated_option_structs_auto.h.inc",
    source_out=GENERATED_SOURCE_ROOT / "c_generated_option_structs_auto.cc.inc",
    families=(
        FamilyConfig(
            struct_name="DBOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_options",
            mode="options",
            enum_types=(
                "InfoLogLevel",
                "WALRecoveryMode",
                "CompressionType",
                "CacheTier",
                "Temperature",
                "PartitionWALUsage",
            ),
            setter_name_overrides=(
                ("compaction_readahead_size", "rocksdb_options_compaction_readahead_size"),
            ),
        ),
        FamilyConfig(
            struct_name="AdvancedColumnFamilyOptions",
            header=INCLUDE_ROOT / "advanced_options.h",
            c_receiver_type="rocksdb_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_options",
            mode="options",
            enum_types=(
                "CompressionType",
                "CompactionStyle",
                "CompactionPri",
                "Temperature",
                "PrepopulateBlobCache",
                "VerifyOutputFlags",
            ),
            setter_name_overrides=(
                (
                    "enable_blob_garbage_collection",
                    "rocksdb_options_set_enable_blob_gc",
                ),
                (
                    "blob_garbage_collection_age_cutoff",
                    "rocksdb_options_set_blob_gc_age_cutoff",
                ),
                (
                    "blob_garbage_collection_force_threshold",
                    "rocksdb_options_set_blob_gc_force_threshold",
                ),
            ),
            getter_name_overrides=(
                (
                    "enable_blob_garbage_collection",
                    "rocksdb_options_get_enable_blob_gc",
                ),
                (
                    "blob_garbage_collection_age_cutoff",
                    "rocksdb_options_get_blob_gc_age_cutoff",
                ),
                (
                    "blob_garbage_collection_force_threshold",
                    "rocksdb_options_get_blob_gc_force_threshold",
                ),
            ),
        ),
        FamilyConfig(
            struct_name="ColumnFamilyOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_options",
            mode="options",
            enum_types=("CompressionType",),
        ),
        FamilyConfig(
            struct_name="WriteOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_writeoptions_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_writeoptions",
            mode="options",
            enum_types=("Env::IOPriority", "Env::IOActivity"),
            setter_name_overrides=(
                ("disableWAL", "rocksdb_writeoptions_disable_WAL"),
            ),
        ),
        FamilyConfig(
            struct_name="FlushOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_flushoptions_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_flushoptions",
            mode="options",
        ),
        FamilyConfig(
            struct_name="FlushWALOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_flushwaloptions_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_flushwaloptions",
            mode="options",
            enum_types=("Env::IOPriority",),
        ),
        FamilyConfig(
            struct_name="EnvOptions",
            header=INCLUDE_ROOT / "env.h",
            c_receiver_type="rocksdb_envoptions_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_envoptions",
            mode="options",
            setter_name_overrides=(
                ("set_fd_cloexec", "rocksdb_envoptions_set_fd_cloexec"),
            ),
            getter_name_overrides=(
                ("set_fd_cloexec", "rocksdb_envoptions_get_fd_cloexec"),
            ),
        ),
        FamilyConfig(
            struct_name="TraceOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_trace_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_trace_options",
            mode="options",
        ),
        FamilyConfig(
            struct_name="BlockCacheTraceOptions",
            header=INCLUDE_ROOT / "block_cache_trace_writer.h",
            c_receiver_type="rocksdb_block_cache_trace_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_block_cache_trace_options",
            mode="options",
        ),
        FamilyConfig(
            struct_name="BlockCacheTraceWriterOptions",
            header=INCLUDE_ROOT / "block_cache_trace_writer.h",
            c_receiver_type="rocksdb_block_cache_trace_writer_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_block_cache_trace_writer_options",
            mode="options",
        ),
        FamilyConfig(
            struct_name="CompactionOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_compaction_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_compaction_options",
            mode="options",
            enum_types=("CompressionType", "Temperature"),
        ),
        FamilyConfig(
            struct_name="CompactRangeOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_compactoptions_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_compactoptions",
            mode="options",
            enum_types=("BottommostLevelCompaction", "BlobGarbageCollectionPolicy"),
            enum_c_type="int",
        ),
        FamilyConfig(
            struct_name="IngestExternalFileOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_ingestexternalfileoptions_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_ingestexternalfileoptions",
            mode="options",
        ),
        FamilyConfig(
            struct_name="OpenAndCompactOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_open_and_compact_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_open_and_compact_options",
            mode="options",
        ),
        FamilyConfig(
            struct_name="ImportColumnFamilyOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_import_column_family_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_import_column_family_options",
            mode="options",
        ),
        FamilyConfig(
            struct_name="SizeApproximationOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_size_approximation_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_size_approximation_options",
            mode="options",
        ),
        FamilyConfig(
            struct_name="LiveFilesStorageInfoOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_livefiles_storage_info_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_livefiles_storage_info_options",
            mode="options",
        ),
        FamilyConfig(
            struct_name="GetColumnFamilyMetaDataOptions",
            header=INCLUDE_ROOT / "metadata.h",
            c_receiver_type="rocksdb_column_family_metadata_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_column_family_metadata_options",
            mode="options",
        ),
        FamilyConfig(
            struct_name="WaitForCompactOptions",
            header=INCLUDE_ROOT / "options.h",
            c_receiver_type="rocksdb_wait_for_compact_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_wait_for_compact_options",
            mode="options",
        ),
        FamilyConfig(
            struct_name="BlockBasedTableOptions",
            header=INCLUDE_ROOT / "table.h",
            c_receiver_type="rocksdb_block_based_table_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_block_based_options",
            mode="options",
            enum_types=(
                "IndexType",
                "BlockSearchType",
                "DataBlockIndexType",
                "ChecksumType",
                "IndexShorteningMode",
                "PrepopulateBlockCache",
                "OptimizeKeyCommonPrefix",
            ),
        ),
        FamilyConfig(
            struct_name="CuckooTableOptions",
            header=INCLUDE_ROOT / "table.h",
            c_receiver_type="rocksdb_cuckoo_table_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_cuckoo_options",
            mode="options",
        ),
        FamilyConfig(
            struct_name="CompactionOptionsUniversal",
            header=INCLUDE_ROOT / "universal_compaction.h",
            c_receiver_type="rocksdb_universal_compaction_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_universal_compaction_options",
            mode="options",
            field_expr_format="{receiver_name}->rep->{field_name}",
            enum_types=("CompactionStopStyle",),
        ),
        FamilyConfig(
            struct_name="CompactionOptionsFIFO",
            header=INCLUDE_ROOT / "advanced_options.h",
            c_receiver_type="rocksdb_fifo_compaction_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_fifo_compaction_options",
            mode="options",
        ),
        FamilyConfig(
            struct_name="BackupEngineOptions",
            header=INCLUDE_ROOT / "utilities/backup_engine.h",
            c_receiver_type="rocksdb_backup_engine_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_backup_engine_options",
            mode="options",
            enum_types=("ShareFilesNaming",),
        ),
        FamilyConfig(
            struct_name="CreateBackupOptions",
            header=INCLUDE_ROOT / "utilities/backup_engine.h",
            c_receiver_type="rocksdb_create_backup_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_create_backup_options",
            mode="options",
            enum_types=("CpuPriority",),
        ),
        FamilyConfig(
            struct_name="RestoreOptions",
            header=INCLUDE_ROOT / "utilities/backup_engine.h",
            c_receiver_type="rocksdb_restore_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_restore_options",
            mode="options",
            enum_types=("Mode",),
        ),
        FamilyConfig(
            struct_name="TransactionDBOptions",
            header=INCLUDE_ROOT / "utilities/transaction_db.h",
            c_receiver_type="rocksdb_transactiondb_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_transactiondb_options",
            mode="options",
            enum_types=("TxnDBWritePolicy",),
        ),
        FamilyConfig(
            struct_name="TransactionOptions",
            header=INCLUDE_ROOT / "utilities/transaction_db.h",
            c_receiver_type="rocksdb_transaction_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_transaction_options",
            mode="options",
        ),
        FamilyConfig(
            struct_name="OptimisticTransactionOptions",
            header=INCLUDE_ROOT / "utilities/optimistic_transaction_db.h",
            c_receiver_type="rocksdb_optimistictransaction_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_optimistictransaction_options",
            mode="options",
        ),
        FamilyConfig(
            struct_name="OptimisticTransactionDBOptions",
            header=INCLUDE_ROOT / "utilities/optimistic_transaction_db.h",
            c_receiver_type="rocksdb_optimistictransactiondb_options_t*",
            c_receiver_name="opt",
            c_prefix="rocksdb_optimistictransactiondb_options",
            mode="options",
            enum_types=("OccValidationPolicy",),
        ),
    ),
)

JOBINFO_GROUP = OutputGroup(
    name="Auto-discovered JobInfo metadata simple",
    header_out=GENERATED_INCLUDE_ROOT / "c_generated_jobinfo_auto.h.inc",
    source_out=GENERATED_SOURCE_ROOT / "c_generated_jobinfo_auto.cc.inc",
    families=(
        FamilyConfig(
            struct_name="FlushJobInfo",
            header=INCLUDE_ROOT / "listener.h",
            c_receiver_type="const rocksdb_flushjobinfo_t*",
            c_receiver_name="info",
            c_prefix="rocksdb_flushjobinfo",
            mode="metadata",
            enum_types=("FlushReason", "CompressionType"),
            enum_c_type="uint32_t",
        ),
        FamilyConfig(
            struct_name="CompactionJobInfo",
            header=INCLUDE_ROOT / "listener.h",
            c_receiver_type="const rocksdb_compactionjobinfo_t*",
            c_receiver_name="info",
            c_prefix="rocksdb_compactionjobinfo",
            mode="metadata",
            enum_types=("CompactionReason", "CompressionType"),
            enum_c_type="uint32_t",
        ),
        FamilyConfig(
            struct_name="SubcompactionJobInfo",
            header=INCLUDE_ROOT / "listener.h",
            c_receiver_type="const rocksdb_subcompactionjobinfo_t*",
            c_receiver_name="info",
            c_prefix="rocksdb_subcompactionjobinfo",
            mode="metadata",
            enum_types=("CompactionReason", "CompressionType"),
            enum_c_type="uint32_t",
        ),
        FamilyConfig(
            struct_name="ExternalFileIngestionInfo",
            header=INCLUDE_ROOT / "listener.h",
            c_receiver_type="const rocksdb_externalfileingestioninfo_t*",
            c_receiver_name="info",
            c_prefix="rocksdb_externalfileingestioninfo",
            mode="metadata",
        ),
        FamilyConfig(
            struct_name="MemTableInfo",
            header=INCLUDE_ROOT / "listener.h",
            c_receiver_type="const rocksdb_memtableinfo_t*",
            c_receiver_name="info",
            c_prefix="rocksdb_memtableinfo",
            mode="metadata",
        ),
    ),
)

METADATA_VIEW_GROUP = OutputGroup(
    name="Auto-discovered metadata view structs simple",
    header_out=GENERATED_INCLUDE_ROOT / "c_generated_metadata_structs_auto.h.inc",
    source_out=GENERATED_SOURCE_ROOT / "c_generated_metadata_structs_auto.cc.inc",
    families=(
        FamilyConfig(
            struct_name="TableProperties",
            header=INCLUDE_ROOT / "table_properties.h",
            c_receiver_type="const rocksdb_table_properties_t*",
            c_receiver_name="props",
            c_prefix="rocksdb_table_properties",
            mode="metadata",
        ),
        FamilyConfig(
            struct_name="CompactionJobStats",
            header=INCLUDE_ROOT / "compaction_job_stats.h",
            c_receiver_type="const rocksdb_compaction_job_stats_t*",
            c_receiver_name="stats",
            c_prefix="rocksdb_compaction_job_stats",
            mode="metadata",
        ),
        FamilyConfig(
            struct_name="CompactionFileInfo",
            header=INCLUDE_ROOT / "listener.h",
            c_receiver_type="const rocksdb_compaction_file_info_t*",
            c_receiver_name="info",
            c_prefix="rocksdb_compaction_file_info",
            mode="metadata",
        ),
        FamilyConfig(
            struct_name="BlobFileAdditionInfo",
            header=INCLUDE_ROOT / "listener.h",
            c_receiver_type="const rocksdb_blob_file_addition_info_t*",
            c_receiver_name="info",
            c_prefix="rocksdb_blob_file_addition_info",
            mode="metadata",
        ),
        FamilyConfig(
            struct_name="BlobFileGarbageInfo",
            header=INCLUDE_ROOT / "listener.h",
            c_receiver_type="const rocksdb_blob_file_garbage_info_t*",
            c_receiver_name="info",
            c_prefix="rocksdb_blob_file_garbage_info",
            mode="metadata",
        ),
    ),
)

OUTPUT_GROUPS = (
    READ_OPTIONS_GROUP,
    OPTION_STRUCTS_GROUP,
    JOBINFO_GROUP,
    METADATA_VIEW_GROUP,
)
MANAGED_FAMILY_NAMES = frozenset(
    family.struct_name for group in OUTPUT_GROUPS for family in group.families
)
AUTO_OUTPUTS = {
    group.header_out.resolve() for group in OUTPUT_GROUPS
} | {group.source_out.resolve() for group in OUTPUT_GROUPS}

FUNCTION_RE = re.compile(r"\brocksdb_[A-Za-z0-9_]+\b(?=\s*\()")
HEADER_INCLUDE_RE = re.compile(r'^#include "(c_api_gen/[^"]+\.inc)"$', re.MULTILINE)
SOURCE_INCLUDE_RE = re.compile(r'^#include "(c_api_gen/[^"]+\.inc)"$', re.MULTILINE)
TOKEN_RE = re.compile(r"[A-Z]+(?=[A-Z][a-z0-9]|_|$)|[A-Z]?[a-z0-9]+|[A-Z]+")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _require_non_empty_string(value: object, context: str, errors: list[str]) -> str | None:
    if not isinstance(value, str):
        errors.append(f"{context} must be a non-empty string")
        return None
    stripped = value.strip()
    if not stripped:
        errors.append(f"{context} must be a non-empty string")
        return None
    return stripped


def load_blocklist(path: Path) -> tuple[dict[str, dict[str, BlocklistEntry]], list[str]]:
    rel_path = display_path(path)
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        return {}, [f"blocklist file not found: {rel_path}"]
    except json.JSONDecodeError as exc:
        return {}, [
            f"invalid blocklist JSON: {rel_path}:{exc.lineno}:{exc.colno}: {exc.msg}"
        ]

    errors: list[str] = []
    blocklist: dict[str, dict[str, BlocklistEntry]] = {}

    if not isinstance(raw, dict):
        return {}, [f"blocklist root must be an object: {rel_path}"]

    schema_version = raw.get("schema_version")
    if schema_version != 1:
        errors.append(
            f"unsupported blocklist schema_version in {rel_path}: {schema_version!r}"
        )

    families = raw.get("families")
    if not isinstance(families, list):
        errors.append(f"{rel_path}: `families` must be a list")
        return {}, errors

    seen_families: set[str] = set()
    for family_index, family_raw in enumerate(families):
        family_context = f"{rel_path}:families[{family_index}]"
        if not isinstance(family_raw, dict):
            errors.append(f"{family_context} must be an object")
            continue
        unknown_family_keys = sorted(set(family_raw) - {"name", "entries"})
        if unknown_family_keys:
            errors.append(
                f"{family_context} has unknown keys: {', '.join(unknown_family_keys)}"
            )

        family_name = _require_non_empty_string(
            family_raw.get("name"), f"{family_context}.name", errors
        )
        if family_name is None:
            continue
        if family_name in seen_families:
            errors.append(f"duplicate blocklist family: {family_name}")
            continue
        seen_families.add(family_name)
        if family_name not in MANAGED_FAMILY_NAMES:
            errors.append(f"unknown blocklist family: {family_name}")
            continue

        entries = family_raw.get("entries")
        if not isinstance(entries, list):
            errors.append(f"{family_context}.entries must be a list")
            continue
        if not entries:
            errors.append(f"{family_context}.entries must not be empty")
            continue

        family_entries: dict[str, BlocklistEntry] = {}
        for entry_index, entry_raw in enumerate(entries):
            entry_context = f"{family_context}.entries[{entry_index}]"
            if not isinstance(entry_raw, dict):
                errors.append(f"{entry_context} must be an object")
                continue
            unknown_entry_keys = sorted(
                set(entry_raw) - {"field", "policy", "reason", "tracking_issue"}
            )
            if unknown_entry_keys:
                errors.append(
                    f"{entry_context} has unknown keys: {', '.join(unknown_entry_keys)}"
                )

            field_name = _require_non_empty_string(
                entry_raw.get("field"), f"{entry_context}.field", errors
            )
            policy = _require_non_empty_string(
                entry_raw.get("policy"), f"{entry_context}.policy", errors
            )
            reason = _require_non_empty_string(
                entry_raw.get("reason"), f"{entry_context}.reason", errors
            )

            tracking_issue_raw = entry_raw.get("tracking_issue")
            tracking_issue: str | None = None
            if tracking_issue_raw is not None:
                tracking_issue = _require_non_empty_string(
                    tracking_issue_raw, f"{entry_context}.tracking_issue", errors
                )

            if field_name is None or policy is None or reason is None:
                continue
            if policy not in BLOCKLIST_POLICIES:
                errors.append(
                    f"{entry_context}.policy must be one of: "
                    f"{', '.join(sorted(BLOCKLIST_POLICIES))}"
                )
                continue
            if field_name in family_entries:
                errors.append(f"duplicate blocklist entry: {family_name}.{field_name}")
                continue

            family_entries[field_name] = BlocklistEntry(
                family_name=family_name,
                field_name=field_name,
                policy=policy,
                reason=reason,
                tracking_issue=tracking_issue,
            )

        if family_entries:
            blocklist[family_name] = family_entries

    return blocklist, errors


def iter_json_docs(text: str) -> Iterable[dict]:
    decoder = JSONDecoder()
    index = 0
    length = len(text)
    while index < length:
        while index < length and text[index].isspace():
            index += 1
        if index >= length:
            return
        obj, index = decoder.raw_decode(text, index)
        yield obj


# clang++ used for AST dumping. Always supplied via --clang (from CLANG_CXX,
# resolved once by build_tools/build_detect_platform); this script is a codegen
# step driven by `make regen-c-api`, not meant to be run by hand, so it does no
# clang detection of its own.
_CLANG_BINARY = ""


def walk_ast(node: dict) -> Iterable[dict]:
    yield node
    for child in node.get("inner", []):
        yield from walk_ast(child)


_ROCKSDB_NS_PREFIX = "rocksdb::"


def _normalize_qual_type(qual_type: str, struct_name: str) -> str:
    # Older clang (e.g. 15) spells a field's type fully qualified -- with the
    # ROCKSDB namespace ("rocksdb::ReadTier") and, for enums nested in the struct
    # being dumped, the enclosing class too ("rocksdb::BlockBasedTableOptions::
    # IndexType"). Newer clang -- the fbcode/CI toolchain the checked-in output is
    # generated with -- spells these minimally ("ReadTier", "IndexType"), which is
    # what the family type tables use. Strip the ROCKSDB namespace and
    # enclosing-struct qualifiers so field discovery is clang-version-independent
    # (a no-op on newer clang). Qualifiers for *other* classes (e.g.
    # "Env::IOPriority") are intentionally preserved.
    qual_type = qual_type.removeprefix(_ROCKSDB_NS_PREFIX)
    qual_type = qual_type.removeprefix(f"{struct_name}::")
    return qual_type


def _dump_record(struct_name: str, header: Path) -> dict:
    cmd = [
        _CLANG_BINARY,
        "-std=c++20",
        "-Wno-pragma-once-outside-header",
        "-I.",
        "-I./include",
        "-x",
        "c++",
        "-Xclang",
        "-ast-dump=json",
        "-Xclang",
        f"-ast-dump-filter={struct_name}",
        "-fsyntax-only",
        str(header.relative_to(ROOT)),
    ]
    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    for doc in iter_json_docs(proc.stdout):
        for node in walk_ast(doc):
            if (
                node.get("kind") in ("RecordDecl", "CXXRecordDecl")
                and node.get("name") == struct_name
                and node.get("completeDefinition")
            ):
                return node
    raise RuntimeError(f"Could not find AST definition for {struct_name}")


def _public_base_names(node: dict) -> list[str]:
    names = []
    for base in node.get("bases", []):
        if base.get("access") != "public":
            continue
        qual_type = base.get("type", {}).get("qualType", "")
        simple = qual_type.split()[-1].split("::")[-1] if qual_type else ""
        if simple:
            names.append(simple)
    return names


def discover_fields(family: FamilyConfig) -> list[tuple[str, str]]:
    # Include fields inherited from public base classes that are not themselves
    # managed families, so a family whose fields live in a shared base (e.g.
    # BackupEngineOptions : CheckpointOrBackupEngineOptions) still exposes them.
    # Base fields precede own fields, matching C++ subobject layout. Managed
    # bases are skipped so each managed family owns exactly its own fields (e.g.
    # ColumnFamilyOptions does not absorb AdvancedColumnFamilyOptions).
    fields: list[tuple[str, str]] = []
    seen_bases: set[str] = set()

    def collect(struct_name: str) -> None:
        node = _dump_record(struct_name, family.header)
        for base_name in _public_base_names(node):
            if base_name in MANAGED_FAMILY_NAMES or base_name in seen_bases:
                continue
            seen_bases.add(base_name)
            collect(base_name)
        # Members of an anonymous union are accessed as direct members. Clang
        # emits the unnamed union record followed by an implicit unnamed
        # FieldDecl for it.
        anon_union_fields: list[dict] = []
        for child in node.get("inner", []):
            kind = child.get("kind")
            if (
                kind in ("RecordDecl", "CXXRecordDecl")
                and child.get("tagUsed") == "union"
                and "name" not in child
            ):
                anon_union_fields = [
                    c
                    for c in child.get("inner", [])
                    if c.get("kind") == "FieldDecl" and "name" in c
                ]
                continue
            if kind != "FieldDecl":
                continue
            if "name" in child:
                members = [child]
            elif child.get("isImplicit"):
                members = anon_union_fields
                anon_union_fields = []
            else:
                members = []
            for member in members:
                fields.append(
                    (
                        member["name"],
                        _normalize_qual_type(
                            member["type"]["qualType"], struct_name
                        ),
                    )
                )

    collect(family.struct_name)
    return fields


def collect_function_names(root_file: Path, include_re: re.Pattern[str]) -> set[str]:
    names = set(FUNCTION_RE.findall(root_file.read_text()))
    for rel in include_re.findall(root_file.read_text()):
        include_path = ROOT / Path(rel)
        if include_path.resolve() in AUTO_OUTPUTS or not include_path.exists():
            continue
        names.update(FUNCTION_RE.findall(include_path.read_text()))
    return names


def collect_existing_bindings() -> tuple[set[str], set[str]]:
    decls = collect_function_names(TEMPLATE_ROOT / "c_base.h", HEADER_INCLUDE_RE)
    defs = collect_function_names(TEMPLATE_ROOT / "c_base.cc", SOURCE_INCLUDE_RE)
    return decls, defs


def normalize_token(token: str) -> str:
    if len(token) > 1 and token.isupper():
        return token
    return token.lower()


def to_c_suffix(name: str) -> str:
    tokens = TOKEN_RE.findall(name)
    return "_".join(normalize_token(token) for token in tokens)


def lookup_override(overrides: tuple[tuple[str, str], ...], field_name: str) -> str | None:
    for override_field_name, override_name in overrides:
        if override_field_name == field_name:
            return override_name
    return None


def setter_name(family: FamilyConfig, field_name: str) -> str:
    override = lookup_override(family.setter_name_overrides, field_name)
    if override is not None:
        return override
    return f"{family.c_prefix}_set_{to_c_suffix(field_name)}"


def getter_name(family: FamilyConfig, field_name: str) -> str:
    override = lookup_override(family.getter_name_overrides, field_name)
    if override is not None:
        return override
    return f"{family.c_prefix}_get_{to_c_suffix(field_name)}"


def scalar_return_type(qual_type: str) -> str | None:
    if qual_type in (
        "uint8_t",
        "uint32_t",
        "uint64_t",
        "int",
        "int64_t",
        "unsigned int",
        "size_t",
        "double",
    ):
        return qual_type
    if qual_type == "SequenceNumber":
        return "uint64_t"
    return None


def build_option_specs(family: FamilyConfig, field_name: str, qual_type: str) -> list[dict] | None:
    expr = family.field_expr_format.format(
        receiver_name=family.c_receiver_name, field_name=field_name
    )
    setter = setter_name(family, field_name)
    getter = getter_name(family, field_name)
    if qual_type == "bool":
        c_type = "unsigned char"
        return [
            {
                "kind": "field_setter",
                "name": setter,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "value_type": c_type,
                "value_name": "v",
                "field": expr,
            },
            {
                "kind": "field_getter",
                "name": getter,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "return_type": c_type,
                "expr": expr,
            },
        ]

    scalar_type = scalar_return_type(qual_type)
    if scalar_type is not None:
        return [
            {
                "kind": "field_setter",
                "name": setter,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "value_type": scalar_type,
                "value_name": "v",
                "field": expr,
            },
            {
                "kind": "field_getter",
                "name": getter,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "return_type": scalar_type,
                "expr": expr,
            },
        ]

    if qual_type == "std::string":
        return [
            {
                "kind": "field_setter",
                "name": setter,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "value_type": "const char*",
                "value_name": "v",
                "field": expr,
            },
            {
                "kind": "string_field_getter",
                "name": getter,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "len_type": "size_t*",
                "len_name": "size",
                "expr": expr,
            },
        ]

    if qual_type == "std::chrono::microseconds":
        return [
            {
                "kind": "field_setter",
                "name": setter,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "value_type": "uint64_t",
                "value_name": "microseconds",
                "field": expr,
                "value_expr": "std::chrono::microseconds(microseconds)",
            },
            {
                "kind": "field_getter",
                "name": getter,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "return_type": "uint64_t",
                "expr": f"{expr}.count()",
            },
        ]

    if qual_type in family.enum_types:
        return [
            {
                "kind": "field_setter",
                "name": setter,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "value_type": family.enum_c_type,
                "value_name": "v",
                "field": expr,
                "value_expr": f"static_cast<decltype({expr})>(v)",
            },
            {
                "kind": "field_getter",
                "name": getter,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "return_type": family.enum_c_type,
                "expr": f"static_cast<{family.enum_c_type}>({expr})",
            },
        ]

    return None


def build_metadata_specs(
    family: FamilyConfig, field_name: str, qual_type: str
) -> list[dict] | None:
    expr = family.field_expr_format.format(
        receiver_name=family.c_receiver_name, field_name=field_name
    )
    fn_name = f"{family.c_prefix}_{to_c_suffix(field_name)}"

    if qual_type == "std::string":
        return [
            {
                "kind": "string_field_getter",
                "name": fn_name,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "len_type": "size_t*",
                "len_name": "size",
                "expr": expr,
            }
        ]

    if qual_type == "Status":
        return [
            {
                "kind": "status_getter",
                "name": fn_name,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "expr": expr,
            }
        ]

    if qual_type == "bool":
        return [
            {
                "kind": "field_getter",
                "name": fn_name,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "return_type": "unsigned char",
                "expr": expr,
            }
        ]

    scalar_type = scalar_return_type(qual_type)
    if scalar_type is not None:
        return [
            {
                "kind": "field_getter",
                "name": fn_name,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "return_type": scalar_type,
                "expr": expr,
            }
        ]

    if qual_type in family.enum_types:
        return [
            {
                "kind": "field_getter",
                "name": fn_name,
                "receiver_type": family.c_receiver_type,
                "receiver_name": family.c_receiver_name,
                "return_type": family.enum_c_type,
                "expr": f"static_cast<{family.enum_c_type}>({expr})",
            }
        ]

    return None


def apply_abi_type_overrides(specs: list[dict] | None) -> list[dict] | None:
    """Pin the public C type of already-shipped functions for ABI stability.

    The generated body still assigns/returns through the real C++ field type
    via static_cast, so only the C signature is held stable. See
    abi_type_overrides.json.
    """
    if not specs:
        return specs
    for spec in specs:
        c_type = ABI_TYPE_OVERRIDES.get(spec.get("name", ""))
        if c_type is None:
            continue
        if spec["kind"] == "field_setter":
            spec["value_type"] = c_type
            # Cast the (ABI-pinned) C value to the actual C++ field type.
            spec["value_expr"] = (
                f"static_cast<decltype({spec['field']})>({spec['value_name']})"
            )
        elif spec["kind"] == "field_getter":
            spec["return_type"] = c_type
            spec["expr"] = f"static_cast<{c_type}>({spec['expr']})"
        else:
            raise ValueError(
                f"ABI type override for {spec['name']} targets unsupported "
                f"spec kind {spec['kind']!r}"
            )
    return specs


def build_specs(family: FamilyConfig, field_name: str, qual_type: str) -> list[dict] | None:
    if family.mode == "options":
        return apply_abi_type_overrides(
            build_option_specs(family, field_name, qual_type)
        )
    if family.mode == "metadata":
        return apply_abi_type_overrides(
            build_metadata_specs(family, field_name, qual_type)
        )
    raise ValueError(f"Unknown family mode: {family.mode}")


def regen_comment_lines(output_group: OutputGroup) -> list[str]:
    return [
        "// To regenerate this file:",
        "//   python3 tools/c_api_gen/auto_simple_bindings.py",
        "",
        f"// Output group: {output_group.name}",
        "",
    ]


def auto_header_banner(output_group: OutputGroup) -> list[str]:
    sources = sorted(
        {
            str(family.header.relative_to(ROOT))
            for family in output_group.families
        }
    )
    lines = [
        "//  Copyright (c) Meta Platforms, Inc. and affiliates.",
        "//  This source code is licensed under both the GPLv2 (found in the",
        "//  COPYING file in the root directory) and Apache 2.0 License",
        "//  (found in the LICENSE.Apache file in the root directory).",
        # "\x40" is "@" -- escaped so this generator source is not flagged as generated
        "// \x40generated",
        "// -----------------------------------------------------------------------------",
        "// Auto-generated by tools/c_api_gen/auto_simple_bindings.py.",
        "// DO NOT EDIT THIS FILE DIRECTLY.",
        "// Sources:",
    ]
    for source in sources:
        lines.append(f"//   - {source}")
    lines.extend(
        [
            f"//   - {display_path(BLOCKLIST_PATH)}",
            "//   - include/rocksdb/c.h",
            "//   - tools/c_api_gen/c_base.cc",
            "// -----------------------------------------------------------------------------",
            "",
        ]
    )
    return lines


def render_group(output_group: OutputGroup, functions_by_family: dict[str, list[dict]], mode: str) -> str:
    lines: list[str] = []
    lines.extend(auto_header_banner(output_group))
    lines.extend(regen_comment_lines(output_group))
    for family in output_group.families:
        section_name = family.section_name or family.struct_name
        lines.append(f"/* {section_name} */")
        lines.append("")
        render = render_decl if mode == "header" else render_impl
        for fn in functions_by_family[family.struct_name]:
            lines.extend(render(fn))
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def classify_function_coverage(
    fn_name: str, decls: set[str], defs: set[str], errors: list[str]
) -> str:
    in_decl = fn_name in decls
    in_def = fn_name in defs
    if in_decl and in_def:
        return "covered"
    if in_decl and not in_def:
        return "missing_def"
    if not in_decl and not in_def:
        return "missing_both"
    errors.append(
        f"partial binding coverage: {fn_name} "
        f"(declared={in_decl}, defined={in_def})"
    )
    return "partial"


def generate_groups(blocklist_path: Path) -> tuple[dict[Path, str], list[str]]:
    blocklist, errors = load_blocklist(blocklist_path)
    decls, defs = collect_existing_bindings()
    outputs: dict[Path, str] = {}

    for output_group in OUTPUT_GROUPS:
        header_generated: dict[str, list[dict]] = {
            family.struct_name: [] for family in output_group.families
        }
        source_generated: dict[str, list[dict]] = {
            family.struct_name: [] for family in output_group.families
        }
        for family in output_group.families:
            fields = discover_fields(family)
            blocked_fields = blocklist.get(family.struct_name, {})
            discovered_field_names = {field_name for field_name, _ in fields}
            for blocked_field_name in sorted(blocked_fields):
                if blocked_field_name not in discovered_field_names:
                    errors.append(
                        "stale blocklist entry: "
                        f"{family.struct_name}.{blocked_field_name} not found in "
                        f"{family.header.relative_to(ROOT)}"
                    )
            for field_name, qual_type in fields:
                if field_name in blocked_fields:
                    continue
                specs = build_specs(family, field_name, qual_type)
                if specs is None:
                    errors.append(
                        "unsupported field in managed family: "
                        f"{family.struct_name}.{field_name} ({qual_type}); "
                        f"extend auto_simple_bindings.py or add an entry to "
                        f"{display_path(blocklist_path)}"
                    )
                    continue
                for spec in specs:
                    coverage = classify_function_coverage(
                        spec["name"], decls, defs, errors
                    )
                    if coverage == "missing_both":
                        header_generated[family.struct_name].append(spec)
                        source_generated[family.struct_name].append(spec)
                    elif coverage == "missing_def":
                        source_generated[family.struct_name].append(spec)

        outputs[output_group.header_out] = render_group(
            output_group, header_generated, "header"
        )
        outputs[output_group.source_out] = render_group(
            output_group, source_generated, "source"
        )

    return outputs, errors


def format_generated_output(path: Path, content: str) -> str:
    if CLANG_FORMAT is None:
        return content
    result = subprocess.run(
        [CLANG_FORMAT, "--assume-filename", str(path)],
        input=content,
        text=True,
        capture_output=True,
        check=True,
        cwd=ROOT,
    )
    return result.stdout


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail instead of writing if outputs are stale",
    )
    parser.add_argument(
        "--blocklist",
        type=Path,
        default=BLOCKLIST_PATH,
        help="path to the checked-in auto-binding blocklist JSON",
    )
    parser.add_argument(
        "--clang",
        default="clang++",
        help=(
            "clang++ binary used for AST dumping (default: clang++). "
            "`make regen-c-api` overrides this with CLANG_CXX from "
            "build_detect_platform."
        ),
    )
    args = parser.parse_args()

    global _CLANG_BINARY
    _CLANG_BINARY = args.clang

    blocklist_path = args.blocklist
    if not blocklist_path.is_absolute():
        blocklist_path = ROOT / blocklist_path

    outputs, errors = generate_groups(blocklist_path)
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 1

    outputs = {
        path: format_generated_output(path, content)
        for path, content in outputs.items()
    }

    if args.check:
        stale = [
            str(path)
            for path, content in outputs.items()
            if not path.exists() or path.read_text() != content
        ]
        if stale:
            for path in stale:
                print(f"stale generated file: {path}", file=sys.stderr)
            return 1
        return 0

    for path, content in outputs.items():
        write_if_changed(path, content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
