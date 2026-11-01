# RocksDB Diff and Pull Request Summary Guidance

This guidance applies to both internal Phabricator diff summaries and external
GitHub pull request summaries for changes under `fbcode/internal_repo_rocksdb/`.

## Goal

Write each diff or pull request summary for a reviewer who has not read the
patch. The summary must explain the behavioral story and the evidence for it,
not merely inventory the edited files or symbols.

Every new or updated diff or pull request summary must clearly cover:

1. Why the change is required.
2. What the previous behavior was, including the failure mode or limitation.
3. What the new behavior is.
4. What changed in the implementation.
5. Safety boundaries, compatibility, and unsupported or intentionally deferred
   cases.
6. Tests performed and known limitations of the validation.

Use explicit headings for a broad, subtle, or high-risk change. A small,
focused change may combine these points into a few paragraphs, but none may be
left implicit when it affects a review decision.

## What Good RocksDB Summaries Make Concrete

- Name the exact conditions that activate a bug or behavior: relevant options,
  call ordering, storage mode, concurrency mode, file format, platform, or
  workload shape. Also state important cases that are not affected.
- Explain the causal chain from the old behavior to its externally observable
  result. For correctness fixes, distinguish a symptom from the underlying
  invariant violation.
- Describe the new contract before implementation mechanics. Then identify the
  essential implementation choices and why they are sufficient.
- Call out API, ABI, serialized-format, behavioral, and performance
  compatibility where relevant. For additive persisted fields, state how older
  data is interpreted and whether older readers remain supported.
- State safety boundaries precisely: defaults, feature gates, fallback paths,
  ownership and lifetime constraints, memory-ordering assumptions, and cases
  deliberately postponed to a follow-up.
- Support performance claims with a baseline, workload and configuration,
  measurement method, and results. Report meaningful tradeoffs, not only the
  winning metric.
- Make the test plan evidence-based. Name the tests and configurations, say
  what behavior each proves, and report failures, timeouts, untested platforms,
  or stale pre-rebase results honestly. For a regression test, say how it fails
  without the fix when that was verified.
- Separate incidental cleanup or a pre-existing test fix from the primary
  change, and explain why it belongs in the diff.

Avoid vague claims such as "improves performance," "fixes an issue," or "tests
pass." Avoid copying the patch into prose, exhaustive file-by-file narration,
raw command output, and claims broader than the evidence.

## Suggested Structure

Adapt this structure to the change; do not keep empty or irrelevant sections.

```text
Summary:

Why the change is required:
<User or system impact, and why the current behavior is inadequate.>

Previous behavior:
<Old contract or mechanism, triggering conditions, and observable result.>

New behavior:
<New contract and externally visible effect.>

Implementation:
<Essential design and code changes; omit routine edit-by-edit narration.>

Safety, compatibility, and limitations:
<Defaults, unaffected paths, compatibility, unsupported cases, and follow-ups.>

Test Plan:
<Exact tests/benchmarks, configurations, what they demonstrate, and gaps.>
```

For a focused bug fix, a cohesive narrative can be clearer than six headings:
problem and root cause, trigger and unaffected cases, fix, then quantified or
counterfactual validation. For a multi-part change, labeled bullets such as
`Fix`, `Add`, and `Change` can make the implementation and review surface easy
to scan.

## Reference Pull Request Summaries

- [PR #15216](https://github.com/meta/rocksdb/pull/15216) is a strong
  focused bug-fix example. It traces stale iterator state to readahead behavior
  and dropped keys, identifies the exact prerequisites and unaffected mmap
  case, gives before/after measurements, and explains how the regression test
  fails when the fix is reverted.
- [PR #15218](https://github.com/meta/rocksdb/pull/15218) is a concise
  example of explicit `Why`, `Previous behavior`, `New behavior`,
  `Implementation`, and `Safety boundaries and unsupported cases` sections.
- Peter Dillinger's [PR #15231](https://github.com/meta/rocksdb/pull/15231)
  is a strong recent multi-part example. Its `Fix`/`Add`/`Change` bullets
  separate the behaviors, its compatibility sentence explains how old files
  read back, and its test plan maps each new persisted property to concrete
  coverage while explaining an adjacent test adjustment.

Use these pull requests as models for reasoning, specificity, and evidence. Do
not copy their length or headings mechanically; scale the summary to the risk
and complexity of the change.
