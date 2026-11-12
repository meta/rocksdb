# RocksDB Diff and Pull Request Summary Guidance

This guidance applies to both internal Phabricator diff summaries and external
GitHub pull request summaries for changes under `fbcode/internal_repo_rocksdb/`.
Release notes and code comments are covered elsewhere in `CLAUDE.md`.

## Goal and audience

A diff summary is an honest, concise argument that the codebase is better with
the change than without it. Include just enough detail, accurately, to
convince a skeptical reviewer who knows RocksDB but not the code involved.
Honesty includes the costs, limitations, and trade-offs; stating them makes
the argument more credible, not less.

The same argument serves a second reader: a future developer reading the
commit history to learn why the code is the way it is, including deliberate
trade-offs and intended future directions. Both readers need the behavioral
story and the evidence for it, not an inventory of edited files or symbols. The
"what" can be recovered from the code; the "why" usually cannot, so the why is
the one element no summary may omit.

Scale everything else to the impact and interest of the change, not its size:
a one-line change to an option default can warrant pages of analysis and
testing. Background can be brief or omitted when readers already have the
context, as when the change continues prior work (link it), builds on an
experimental feature, or touches niche code. Do not pad with an inventory of
related existing features.

## Choosing the detail

Most of the judgment in a summary is which details to include, and how many.
The test for each detail: would the skeptical reviewer need it to be
convinced, or to avoid being misled? If they would not miss it, cut it.

- **Company-confidential data stays out, in the summary and the test plan,
  even when the test above says to include it.** Most importantly, never give
  the dollar cost, scale, or hardware mix of company assets, such as fleet,
  host, or shard counts, storage or dollar costs, or which hardware
  generations are deployed. Benchmark results on specified host types are
  fine. Other internal context, such as internal product names, incident
  details, or how an internal user hit the problem, belongs in a diff comment.
  An `Internal:` line at the end of the summary is stripped when an
  internally authored commit is exported to GitHub, but do not rely on it for
  anything sensitive; pull requests imported from GitHub have no such filter.
- **Changing existing behavior that has nuance:** err toward detail about that
  behavior: what it does, under which conditions, and why it was built that
  way. Readers need that detail to understand and justify the change.
  Over-simplifying tends to overstate the scope of a bug and to make earlier
  work look worse than it was. Say where the problem shows up for users (the
  workload or scenario that hit it), and link the earlier changes that shaped
  the behavior.
- **Adding something new:** keep the motivation and the description of its
  capabilities and characteristics simple, even when the implementation is
  complex. Describe what it offers and what it costs, not how each part works.
  When it plugs into an existing integration point (an existing interface,
  extension point, thread pool, or option), name that point instead of
  explaining it.
- **Background:** include only what this change depends on. A rename needs
  what the old name suggests and why that misleads, not how the renamed thing
  works or where it came from.
- **Small, low-risk changes** (a rename, a narrow cleanup, a test fix) usually
  need one to three short paragraphs: why, what changed, and what did not.
  Add no separate sections unless something would surprise a reviewer.
- Leave internal mechanics to the code: locking, data-structure details,
  generated-code and tooling plumbing, and which status each rejection
  returns. Include them only when a reader must act on them or needs them to
  judge risk; otherwise they belong, if anywhere, in the optional
  implementation details.
- Most changes need a few paragraphs. Detail that fails the test above makes
  the summary worse, not more complete.

## Narrative order

For a change to existing behavior, tell the story in this order, not
necessarily with all of these nor with these exact headings:

1. **Background** that a typical RocksDB user or developer may not know or
   readily recall, chosen so that it leads to the problem.
2. **The problem**: the tension, gap, or failure that the background reveals.
3. **Current behavior** in detail: the conditions that activate it (options,
   call ordering, storage mode, concurrency, file format, platform, workload)
   and important cases that are not affected.
4. **New behavior**: the new contract and its externally visible effect,
   before implementation mechanics.
5. **How and why**: the essential implementation choices, why they are
   sufficient, and alternatives considered and rejected, with reasons.
6. **Consequences and loose ends**: side effects, compatibility, limitations,
   and follow-up work.
7. **Notable implementation details**: notes by file or component,
   especially where it is not obvious why a file is involved.

A new feature whose essence makes its motivation obvious may lead with what is
new. A focused fix often reads best as a few paragraphs in the same order:
problem and root cause, trigger and unaffected cases, fix, evidence.

Use headings once a summary grows beyond about three paragraphs. If the first
section needs a heading, call it `Background`, not "Why the change is
required", which presumes the reader already knows enough to see the need.
Labeled bullets such as `Fix`, `Add`, and `Change` suit multi-part changes.
Apart from `Background` and `Notable implementation details`, headings can
describe the content rather than simply applying the labels above.
Headings and bullets organize the story; they do not replace it. File-by-file
notes belong in the optional implementation details, not in place of the
story, and no part of the summary should restate the patch in prose.

## What to make concrete

- For a correctness fix, trace the causal chain from the old behavior to its
  externally observable result, and distinguish the symptom from the
  underlying invariant violation. Answer the reader's first question directly,
  such as whether existing data could already be affected, and in which
  workloads.
- When useful, say how a bug was found and what testing gap let it through. If
  the origin should not be public, a task number is enough.
- For a revert, say why, and what kind of workload or operation regressed.
- Call out API, ABI, serialized-format, behavioral, and performance
  compatibility; for additive persisted fields, how older data reads back.
  Compatibility is relative to released versions: when the change revises
  something added since the last release branch was cut, say so, and say how
  that was checked, instead of describing a break or adding a shim.
- State safety boundaries precisely: defaults, feature gates, fallback paths,
  ownership and lifetime constraints, memory-ordering assumptions.
- Link prior work the change builds on rather than re-explaining it, so the
  history keeps a throughline.
- Label small "bonus" inclusions, such as a documentation fix, as bonus (not
  necessarily related). Explain any larger bundled change and why it belongs.
- Be transparent about imperfection. Practically no change is perfect: record
  known gaps, trade-offs accepted, and cases deliberately deferred. Qualify
  follow-up work by how firm it is: planned, intended, possible, or
  aspirational.

## Terminology

Many ordinary words have specific meanings in RocksDB. Do not use them for
other things, even in one-off prose, unless they consistently carry a
qualifier that removes the ambiguity.

| Word | RocksDB meaning | For anything else |
|---|---|---|
| snapshot | `DB::GetSnapshot()` read view | "file snapshot", "MANIFEST snapshot", "copy" |
| timestamp | user-defined timestamp | "unix timestamp", "wall-clock time" |
| sequence | sequence number | "event sequence", "probe sequence" |
| version | `Version`, `format_version`, entries for one user key | "RocksDB release", or say which |
| prefix | `prefix_extractor` prefix; delta-encoding shared prefix | "user-key prefix", "internal-key prefix" |
| flush | memtable flush | name the operation, e.g. `FlushWAL()` |
| pin | keep a block, value, or `SuperVersion` alive | "bound to a core", "covered by tests" |
| filter | Bloom/Ribbon filter; compaction filter | always say which |
| recovery | `DB::Open()` recovery; `DB::Resume()` resumption | name the entry point |
| level | LSM level | "compression level", "log level" |
| data block | the block kind holding key-value entries | "SST blocks", or list the block kinds |
| decode | parse an encoded format | "decompress" for compression |

When the summary needs new shorthand, check that the word is not already a
RocksDB term, define it at first use, keep any qualifier on every use, and
make sure it also reads correctly as plain English. Use exact names for APIs,
options, and properties when precision matters, e.g. `file_creation_time`
rather than "creation time".

## Clarity

- Make clear whether each statement describes the state before or after the
  change. A paragraph break between the two often helps.
- Do not use a bare "this" as a pronoun; write "this change" or "this option".
- The summary must stand on its own without the title, and must explain the
  title's headline claim.
- Describe the final change, not its development history: no diff version
  numbers, rebases, or conflicts. When problems with an earlier attempt help
  motivate the chosen solution, outline them in a clearly labeled passage so
  they are not mistaken for the pre-change or post-change state. "Previous
  behavior" always means the upstream code before the change.
- Prefer measured facts to value judgments about old code, and avoid vague
  claims such as "improves performance" or "fixes an issue", or claims broader
  than the evidence.
- Present non-trivial numbers in markdown tables, with each column defined,
  units given, and the baseline stated for every comparison.
- Write for open source, in the summary and the test plan: no local paths,
  internal names or jargon that outside readers cannot follow, or references
  to earlier versions of the same diff. For confidential data, see "Choosing
  the detail".

## Test Plan

Make the test plan evidence, in proportion to risk.

- Name the new or modified tests and configurations and the behavior each
  proves. For a regression test, say how it fails without the fix when that
  was verified. If relying on unmodified existing tests, say which ones and
  why they cover the change.
- For performance claims, give the baseline, workload and configuration,
  measurement method, and results, including meaningful trade-offs.
- Report failures, timeouts, untested platforms, and stale results honestly.
- Omit what CI already shows or what wastes the reader's time: "existing tests
  pass", "N tests passed", local runs of CI-covered tests, format or build
  checks, and references to files that are not committed.
- A minimal test plan is fine when breakage would show up at compile time (as
  when removing an API) or when there is no functional change. Features and
  behavior changes need real evidence, often including crash test coverage.

## Formatting for both GitHub and Phabricator

The same text renders differently in the two tools. Surround tables with
blank lines, and check them after GitHub converts the commit message into a
pull request description. A paragraph indented to continue a list item after
its nested list renders as part of that item on GitHub but not on
Phabricator, so avoid relying on it.

Both tools render a collapsible section, which suits long supporting material
such as in-depth explanations, thorough benchmark reports, detailed
alternatives considered, and file-by-file implementation notes. Start it with
a `<details>` line followed by a one-line caption such as
`<summary>**Benchmark details:** configurations, raw results</summary>`, then
a blank line, the material (tables, code blocks, longer text), another blank
line, and a `</details>` line. Keep the story readable without expanding
anything.

## Reference summaries

- [PR #15583](https://github.com/meta/rocksdb/pull/15583), a large change:
  background on why SST files need three different times leads to the
  problem; previous behavior is listed case by case; headings separate new
  behavior, implementation, and compatibility; limitations are recorded.
  The summary is long because the PR is a collection of interlinked changes
  that affect quite a few features and behaviors in subtle ways.
- [PR #10408](https://github.com/meta/rocksdb/pull/10408), a refactor:
  problem, solution, details. It defines precise terms to replace an
  ambiguous one; a short test plan fits a change with no functional effect.
- [PR #14732](https://github.com/meta/rocksdb/pull/14732), a focused bug fix:
  previous behavior and its consequence, why the input is invalid, the scope
  of the fix, and a regression test covering every entry point.

Use these as models for reasoning and proportion, not as templates to copy.
