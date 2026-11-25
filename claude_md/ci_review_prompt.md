# Code Review Workflow — CI Prompt

> **History:** This prompt was written for GitHub Actions workflows that
> posted AI reviews on pull requests. Those workflows have been removed; find
> them with `git log --diff-filter=D -- .github/workflows/claude-review.yml`.
> The prompt is still used for local reviews through
> `claude_md/manual_code_review.md`, which overrides the CI-specific
> instructions below (PR comment output, `review-findings.md`, turn limits).

## Purpose
Thorough multi-agent code review for RocksDB commits following CLAUDE.md guidelines,
with structured codebase exploration, inter-agent debate, and verification.

## Behavioral Compatibility Review Perspective

Behavior changes are human judgment calls. They must be intentional,
well-documented (including release notes), and researched for potential
downstream production impact. Agents and reviewers must call out concrete gaps,
ambiguity, or adverse impacts in the change's intent, documentation, and
production-impact analysis.

When a diff changes how an existing accepted public or serialized input is
interpreted—including an undocumented, discouraged, or historically ignored
value—reviewers must:
- Compare the old and new observable behavior, including performance and
  resource impact.
- Check that the rationale and user-facing documentation clearly describe the
  change.
- Identify known direct callers and stored configurations that may depend on
  the previous behavior.
- Check for appropriate regression coverage, or a documented reason why such
  coverage is not practical.

A behavior change is not automatically a defect. Report a finding when the
intent, documentation, downstream-impact analysis, or mitigation has a concrete
gap or ambiguity that could lead to an unintended compatibility regression.

## DB Mutex Review Perspective

For every non-trivial change, determine whether the changed code, or any
transitive callee it adds or changes, can run while `DBImpl::mutex_` is held.
State the result explicitly, including when the DB mutex is not affected.

`DBImpl::mutex_` is the DB-wide metadata and coordination mutex. It protects
cross-object invariants involving `ColumnFamilyData`, memtable and
`SuperVersion` publication, `VersionSet`, snapshots, flush/compaction queues,
background-job state, and file lifetime. It is shared by all column families
and can be a hot lock. It is not the filesystem `db_lock_`, and it should not
replace data-path synchronization already provided by `WriteThread`,
`wal_write_mutex_`, atomics, referenced `SuperVersion`s, or component-local
synchronization.

When a DB-mutex critical section is affected:

### Locking and correctness

- Trace the full call path and mark the exact lock and unlock boundaries. Do
  not inspect only the lines that directly acquire the mutex.
- State the locking contract. Helpers that require the lock should document it
  and normally use `mutex_.AssertHeld()`. Check whether each caller expects the
  lock to be held on return.
- Treat calls that can release and re-acquire the mutex as concurrency
  boundaries. Examples include `VersionSet::LogAndApply()`, condition-variable
  waits, write-thread coordination, and notification helpers. Revalidate state
  inspected before such a call.
- Keep atomic metadata transitions under the mutex. Examples include moving a
  memtable between mutable and immutable state, reserving compaction inputs,
  installing a `Version` or `SuperVersion`, updating scheduling counters and
  queues, and changing file-lifetime protection.
- For expensive work, prefer a prepare/run/install structure: reserve and pin
  the required state while locked, do CPU or I/O work while unlocked, then
  re-lock, validate, and publish. Before unlocking, copy mutable options and
  acquire every required object reference or lifetime reservation.
- Check object lifetime across every unlocked region. Do not use a raw
  `ColumnFamilyData*`, `Version*`, memtable, file metadata pointer, or DB-owned
  container element after unlocking unless an explicit reference,
  `SuperVersion`, job object, or pending-output reservation keeps it alive.
- Preserve the snapshot/input-selection boundary. Flush and compaction must not
  miss a snapshot whose visible key versions they may remove. Missing a newly
  created snapshot is a correctness bug; retaining a recently released
  snapshot is conservative but safe.
- Never invoke untrusted or re-entrant code while holding the mutex. The mutex
  is non-reentrant, so a callback into RocksDB can self-deadlock.
- Check lock ordering. `options_mutex_` is acquired before the DB mutex. When
  both the DB mutex and `wal_write_mutex_` are needed, the DB mutex is acquired
  first. Establish and document the order for other locks.
- Keep cleanup outside the critical section when possible. File deletion,
  `JobContext::Clean()`, expensive `SuperVersion` destruction, logging, and
  listener callbacks should normally run after protected state is detached or
  snapshotted.
- Do not add the DB mutex to steady-state point reads or the normal write data
  phase without strong justification. Reads normally use a referenced
  `SuperVersion`; writes use `WriteThread` and component-specific
  synchronization.

### Critical-section performance

- Compare the CPU and I/O performed under the mutex before and after the
  change. Include call frequency, loop bounds, input-size scaling, hidden work
  in callees, and worst-case cost.
- Account for filesystem or network I/O, waits, callbacks, logging and
  formatting, allocation, container growth, copies, sorting, scans, reference
  release and destructors, cache misses, and repeated metadata lookups.
- Do not allow a significant regression in mutex-held work without clear
  justification, hold-time or contention measurements, and representative
  benchmarks.
- Treat unavoidable work under the mutex as hot-path code and keep it highly
  optimized. Precompute, allocate, copy, and perform I/O outside the lock;
  remove repeated scans and lookups; cache or batch work where practical; and
  keep locked operations O(1) or tightly bounded when possible.
- Treat new blocking I/O, waits, callbacks into user or plugin code, unbounded
  loops, or work that scales with DB size, files, column families, snapshots,
  or pending jobs as a blocker unless it is unavoidable and supported by strong
  measurements and explicit design justification.

### Validation and evidence

- Test unlock windows with sync points rather than sleeps. Cover relevant races
  with memtable switches, snapshot create/release, column-family drop,
  flush/compaction install, shutdown, and background errors. Run TSAN where
  appropriate.
- Distinguish wait time from hold time. `PerfContext::db_mutex_lock_nanos` and
  `DB_MUTEX_WAIT_MICROS` primarily expose acquisition wait; use contention
  profiles or scoped instrumentation to find long critical sections.
- Report affected critical sections, before/after cost, workload assumptions,
  optimization choices, and remaining risk. If exact measurement is not
  practical, provide a conservative static bound and do not claim no regression
  without evidence.

IMPORTANT — Where the review goes: Your FINAL RESPONSE TEXT is the review.
The PR comment is generated verbatim from your final response, so the
complete, polished review MUST appear there in full. Do NOT end your turn with
a meta-summary such as "the review has been written to review-findings.md" —
paste the entire review as your response.

review-findings.md is a RECOVERY BACKSTOP, not the primary output: after
completing EACH major phase (Setup, Codebase Context, Initial Review, Debate,
Synthesis), append your current findings to it with the Write tool, and after
the final synthesis overwrite it with the complete review. This preserves
partial results if the turn limit interrupts the run (a recovery step reads
this file); it does NOT substitute for outputting the full review as your
final response.

## Workflow Steps

### 1. Setup Phase
- Read CLAUDE.md to understand review guidelines
- Identify the commit/diff/PR to review
- The PR diff is provided below in this prompt. The repository is already
  checked out — use local file search tools (Grep, Glob, Read) to explore.
- Parse the changed files and categorize them (API, core logic, tests, etc.)
- Read all changed files to build full context

### 2. Codebase Context Phase (CRITICAL — Do Not Skip)

**Why this matters**: Review agents that only see the diff miss systemic bugs.
For example, a change to an iterator's return value might look correct in
isolation but break multi-SST iteration 5 layers up the call stack. A change
to error handling might silently drop errors that a caller 3 levels up relies
on for recovery. This phase builds the context that prevents those blind spots.

Go deep. Surface-level reading leads to reviews that miss caching layers,
existing helper functions, or concurrency requirements. The most expensive
failure mode is findings that look correct in isolation but miss how the
change breaks the surrounding system.

The team lead spawns one or more research agents to perform the following
analyses using local file search tools (Grep, Glob, Read). The research
must be written to `context.md` BEFORE any review agent is spawned.

#### 2a. Subsystem Deep-Read
Read the changed files AND their surrounding subsystem **in depth** — not just
signatures, but actual logic, edge cases, data flows, locking protocols,
concurrency patterns, and interactions between components.

For each changed file, also read:
- The header that declares its interface (if the change is in a .cc file)
- The sibling implementation that does the same thing the "standard" way
  (e.g., for a UDI iterator change, read how `IndexBlockIter` handles the
  same scenario — it's the reference implementation)
- The wrapper/adapter layer that sits between this code and its callers
- Any existing tests to understand what behaviors are currently guaranteed

#### 2b. Caller-Chain Analysis (upstream impact)
For each changed function/method, trace the call chain UPWARD 3-5 levels to
understand who consumes the changed behavior and what invariants they rely on.
**Focus on control flow decisions** — if/else branches, while loop conditions,
early returns — that depend on the changed return values or state.

**How to do this**:
- Search for callers of each changed function (search for the function name, the
  class method, virtual overrides)
- For each caller, search for ITS callers, repeating 3-5 levels up
- At each level, read the actual code to understand the control flow decision
- Stop when you reach a "terminal" caller (e.g., user-facing API, top-level
  iterator) or when the propagation is clearly safe

#### 2c. Callee-Chain Analysis (downstream dependencies AND side effects)
For each changed function, trace what it calls and whether those calls have
new preconditions or changed semantics. **Critically, trace SIDE EFFECTS —
not just return values.** Many bugs hide in side effects on shared state
(counters, sequence numbers, flags) that are invisible if you only check
return values.

**For each callee, ask:**
1. What does it RETURN? (the obvious check)
2. What shared state does it MUTATE? (the non-obvious check)
3. Under what PRECONDITIONS does it operate correctly?
4. Are those preconditions met in the new calling context?

#### 2d. Sibling Implementation Comparison
Identify the "reference" or "standard" implementation that does the same thing
the changed code does, and compare their behavior. This is critical for plugin/
extension APIs where the implementation must match an implicit contract.

#### 2e. Invariant Documentation
Document the key invariants that the changed code must maintain. Read existing
comments, assertions, and test expectations to discover these invariants.

#### 2f. Related Functionality and Existing Conventions
Identify related functionality that already exists in the codebase:
- Are there existing helper functions the change should use (or is duplicating)?
- Are there existing patterns for the same kind of change (e.g., how other
  Customizable subclasses handle deprecation)?
- Are there existing tests that test similar scenarios (e.g., multi-SST
  iteration tests that could be extended)?

#### 2g. Cross-Component Data Consumer Analysis (CRITICAL — common blind spot)

**Why this is separate from caller-chain analysis:** Sections 2b-2c trace
who CALLS the changed functions. This section traces who CONSUMES the data
that the changed code PRODUCES. These are often completely different
subsystems.

When the change writes data to a shared structure (memtable, SST block, cache
entry, statistics counter), ask: **"Who reads this data, under what rules,
and do those rules match the writer's assumptions?"**

**How to do this:**
1. Identify every piece of data the change WRITES (e.g., a range tombstone
   entry, a counter update, a metadata field)
2. For each, search the codebase for ALL readers of that data structure
3. For each reader, check:
   - Does the reader apply visibility rules (seqno, read_callback, snapshot)?
   - Are those rules compatible with the writer's assumptions?
   - Does the reader assume invariants about the data (e.g., seqno ordering,
     monotonicity) that the writer might violate?

#### 2h. Alternative Execution Contexts

The same code path may run in different contexts with different assumptions.
**Enumerate all contexts and check each one.** This is where "works in the
common case but breaks in edge configurations" bugs hide.

For RocksDB, always check whether the changed code interacts differently with:

| Context | Key difference | Common failure mode |
|---------|---------------|-------------------|
| WritePreparedTxnDB / WriteUnpreparedTxnDB | `read_callback_` controls visibility, not just seqno | Visibility bypass |
| ReadOnly DB / SecondaryInstance | No mutable memtable, writes not allowed | Null pointer or no-op needed |
| CompactionService / Remote compaction | Different process, serialized state | Serialization mismatch |
| User-defined timestamps | Extra dimension in key comparison and visibility | Wrong ordering |
| MemPurge | Memtable-to-memtable, not memtable-to-SST | Missing data |
| Column family with BlobDB | Values may be in blob files, not inline | Missing dereference |
| Snapshots (old, held long-term) | Snapshot seqno may be far behind current state | Metadata corruption |
| Concurrent writers (allow_concurrent_memtable_write) | Lock-free paths vs locked paths | Lost updates |
| FIFO / Universal compaction | Different compaction invariants than Level | Wrong assumptions |
| Prefix seek / total order seek | Different iterator behavior | Wrong iteration results |

**For each context, ask:**
1. Does the code execute in this context? (Check constructor, feature flags)
2. If yes, do the assumptions still hold?
3. If not, should the feature be disabled in this context?

#### 2i. Assumption Stress-Testing (the "logically X" audit)

When the change claims a property (e.g., "logically redundant," "no-op in
this case," "safe because X"), **systematically break it:**

1. **State the claim precisely.** E.g., "The inserted range tombstone is
   logically redundant — it covers only keys already deleted by point
   tombstones."
2. **List the preconditions** that must be true for the claim to hold.
3. **For each precondition, construct a counterexample** — a concrete scenario
   where the precondition is violated.
4. **Verify each counterexample against the code.** Does the code guard
   against it? If not, it's a bug.

**Anti-pattern: treating asserts as proofs.** When you see an assert, do NOT
conclude "the invariant holds." Instead ask: "Can I construct an input where
this assert fires?" If yes, it's a bug (the assert catches it in debug but
release builds silently corrupt). If you cannot construct a counterexample
after trying, document WHY it's impossible.

#### 2j. Write Context Document
Write the complete analysis to `context.md` in the review folder. This document
is provided to ALL review agents as part of their prompt.

**Context document template**:
```markdown
# Codebase Context for Review

## How the Relevant Subsystem Works Today
[Detailed description of the subsystem architecture, data flows,
and component interactions. Not just "what" but "how" and "why."]

## Changed Functions and Their Call Chains

### function_name() (file:line)
**What changed**: [brief description of the behavioral change]

**Upstream callers** (who depends on this):
1. CallerA::method() (file:line) — uses return value to decide X
2. CallerB::method() (file:line) — passes result to CallerC
3. CallerC::method() (file:line) — THE CRITICAL DECISION POINT: ...

**Downstream callees** (what this depends on):
1. calleeA() — behavior unchanged
2. calleeB() — NEW dependency, requires X

**Sibling implementation** (how the "standard" version handles this):
- StandardImpl::method() does Y at file:line, which ensures invariant Z
- The changed code must match or document any deviation

**Key invariants**:
- Must never return X when Y is true because CallerC will...
- Must always set Z before returning because CallerB assumes...

## System Architecture Context
[How the changed components fit into the overall system]

## Known Invariants That Must Be Preserved
1. ...
2. ...

## Existing Conventions and Related Code
- [helper functions, patterns, existing tests that are relevant]

## Cross-Component Data Consumers
For each piece of data the change WRITES to a shared structure:
### [data item] written to [structure]
**Writer assumptions**: [what the writer assumes about how this data is consumed]
**All readers**:
1. ReaderA::method() (file:line) — reads under [visibility rules]
   - Compatible with writer? YES/NO. If NO, explain the mismatch.
2. ReaderB::method() (file:line) — reads under [different rules]
   - Compatible with writer? YES/NO.

## Alternative Execution Contexts
| Context | Does code execute? | Assumptions hold? | Action needed? |
|---------|-------------------|-------------------|----------------|
| WritePreparedTxnDB | YES/NO | YES/NO | [disable/guard/safe] |
| Old snapshots | YES/NO | YES/NO | ... |
| User-defined timestamps | YES/NO | YES/NO | ... |
| ReadOnly DB | YES/NO | YES/NO | ... |
| ... | ... | ... | ... |

## Assumption Stress Test
### Claim: "[the design claim, e.g., logically redundant]"
**Preconditions for claim to hold:**
1. [precondition] — counterexample: [scenario]. Guarded? YES/NO.
2. [precondition] — counterexample: [scenario]. Guarded? YES/NO.

## Potential Pitfalls
- [specific scenarios where the change could interact badly with
  the surrounding system, identified from the caller-chain analysis]
```

### 3. Initial Review Phase (Parallel)
Create a team and spawn 5 review agents in parallel. **Include the context
document from Phase 2 in each agent's prompt.** Each agent writes findings
to its own file and sends a summary message to the team lead.

Each agent's prompt should include:
```
## Codebase Context (READ THIS FIRST)
[paste or reference the context.md file]

You MUST consider how your findings interact with the upstream callers
and system invariants documented above. A finding that looks correct
in isolation may be a critical bug when you consider the full call chain.
```

#### Agent: Design & Approach Reviewer
- Read the commit message deeply to understand the problem being solved
- Analyze whether this is the right approach to the problem
- Consider alternative designs and trade-offs:
  - Are there simpler solutions that achieve the same goal?
  - Does the approach introduce unnecessary complexity?
  - Would a different data structure or algorithm be more appropriate?
- Evaluate whether the change follows existing architectural patterns or
  introduces new patterns that may be inconsistent
- Assess the scope: is the change too large? Should it be split into
  smaller, independently reviewable pieces?
- Check if the approach has precedent in the codebase or in the academic
  literature it references (e.g., SuRF paper)

#### Agent: Correctness Reviewer
- Thread safety and concurrency analysis
- Error handling and propagation via Status type
- Edge cases (empty inputs, overflow, boundary conditions)
- Data corruption scenarios
- Logic correctness of new algorithms
- **Behavioral contract changes**: Do return value semantics match what
  upstream callers expect? (Use the caller-chain analysis from context.md)
- **Callee side effects**: Does the change call functions that mutate shared
  state (counters, seqnos, metadata)? Are the mutations correct for the new
  calling context? (Use the callee-chain analysis from context.md)
- Apply the DB Mutex Review Perspective above to locking contracts, unlocked
  object lifetimes, stale-state validation, lock ordering, and snapshot
  boundaries.

#### Agent: Cross-Component & Adversarial Reviewer

This agent exists because the most critical bugs hide at component boundaries,
not within a single component. It uses a fundamentally different methodology
than the correctness reviewer: instead of verifying "does the code do what it
intends?", it asks "what breaks when we change the assumptions?"

**Data-flow analysis:** Perform the cross-component data consumer analysis
described in section 2g. For every piece of data the change WRITES to a shared
structure, trace ALL READERS and verify their visibility rules are compatible
with the writer's assumptions. This is a DATA-FLOW question, not a
CONTROL-FLOW question — readers may be in completely different subsystems.

**Alternative execution contexts:** Use the canonical table from section 2h.
Enumerate all contexts where the changed code executes and verify assumptions
hold in each.

**Assumption stress-testing and assert-breaking:** Follow the methodology from
section 2i. Identify every design claim, enumerate preconditions, construct
counterexamples. Treat asserts as hypotheses to break, not proofs.
**Red flag words**: "logically redundant", "safe because", "no-op",
"always true", "cannot happen", "invariant holds"

**Callee side-effect audit:** Perform the callee side-effect audit described
in section 2c. For every callee, ask what it RETURNS and what it MUTATES.

Write findings to `findings-cross-component.md`.

#### Agent: Invariant Adversary

This agent does NOT read the diff in detail. It takes the feature summary and
systematically tries to break it. While other agents verify "does the code do
what it says?", this agent asks "what existing system invariants does this
feature violate?" and "under what inputs do the design claims fail?"

**Steps 1-4: Assumption stress-testing.** Follow the methodology from section
2i: extract design claims, enumerate preconditions, construct counterexamples,
and attempt to break every assert. Be exhaustive about input parameter ranges,
shared state configurations, and concurrent interleaving.

**Step 5: Callee side-effect audit.** Perform the callee side-effect audit
described in section 2c. For every function the changed code calls, list ALL
mutations to shared state (atomic CAS loops, counter increments, flag/metadata
updates, cache invalidation). A function returning `Status::OK()` does NOT
mean its side effects are correct.

Write findings to `findings-invariant-adversary.md`.

#### Agent: Caller-Context Auditor

This agent traces BACKWARD from every entry point the changed code hooks into.
While other agents read the changed code forward ("what does it do?"), this
agent enumerates WHO invokes it and WITH WHAT parameter ranges.

The key insight: a function that is correct for all *typical* inputs may be
catastrophically wrong for *atypical-but-reachable* inputs. This agent's job
is to find the atypical inputs.

**Step 1: Identify entry points.** List every function/constructor/method in
the changed code that receives external input (parameters, config values,
pointers to shared state). Include:
- Constructor parameters (e.g., `active_mem`, `read_callback`, `sequence`)
- Virtual method overrides that callers invoke polymorphically
- Functions called from multiple subsystems

**Step 2: Enumerate all callers (3-5 levels up).** For each entry point,
search the codebase for ALL callers. Do NOT stop at the first caller — trace
the full call chain. Use `Grep` and `Glob` to find:
- Direct callers of the function
- Callers of wrapper/adapter functions that forward to it
- Factory functions that construct the object
- Virtual dispatch sites (search for the base class method name)

**Step 3: Parameter range analysis.** For each caller, determine:
- What values does it pass for each parameter?
- Under what conditions does it call this function?
- Are there callers that pass unusual values? (e.g., `kMaxSequenceNumber`,
  `nullptr`, old snapshot seqnos, non-null `read_callback_`)

Build a parameter range table for each entry point, listing all callers
and the values they pass for each parameter.

**Step 4: Configuration matrix.** Enumerate option/config combinations that
affect the changed code path:
- Which options enable/disable the feature?
- Which options change the execution context? (e.g.,
  `allow_concurrent_memtable_write`, `use_trie_index`)
- Are there option combinations that create contradictions?

**Step 5: Cross-reference with context.md.** Compare your caller analysis
with the invariants and execution contexts documented in context.md. Flag
any caller that violates an assumed precondition.

Write findings to `findings-caller-audit.md`.

#### Agent: Performance Reviewer
- Memory allocation on hot paths
- Unnecessary copies (string, buffer)
- Cache efficiency and data locality
- Loop optimization opportunities
- Branch prediction (LIKELY/UNLIKELY)
- Zero-overhead design verification
- Apply the DB Mutex Review Perspective above, with special focus on transitive
  CPU and I/O, before/after cost, critical-section hold time, and benchmark
  evidence

#### Agent: API & Compatibility Reviewer
- Public API backwards compatibility
- API consistency with existing patterns
- Documentation completeness
- Deprecation policy compliance
- Forward compatibility (struct extensibility)
- Apply the Behavioral Compatibility Review Perspective above to changes in
  observable semantics and implicit caller contracts.

#### Agent: Serialization & Deserialization Reviewer
- Format correctness and versioning
- Alignment requirements
- Integer overflow in size computations
- Bounds checking on untrusted data
- Crafted input attack vectors

#### Agent: Test Coverage Reviewer
- Edge case coverage
- Failure mode testing (corruption, truncation)
- Round-trip testing (build → serialize → deserialize → verify)
- Integration test coverage
- **System-level test coverage**: Are the caller-chain interactions tested?
  (e.g., multi-SST iteration with UDI, level compaction with new behavior)
- Test quality (no flaky patterns, good assertions)

### 4. Round-Robin Debate Phase
After all 5 agents complete their initial review, the team lead orchestrates
a structured debate:

#### Step 4a: Share findings
- Team lead sends each agent a summary of ALL other agents' findings
  (or instructs each agent to read the other agents' findings files)

#### Step 4b: Critique round (2 rounds)
Each agent reviews the findings from the other agents and sends messages to
challenge, support, or refine them:

- **Challenge**: "I disagree with [agent] [finding] because [evidence]."
- **Support**: "I independently found the same issue — this confirms it."
- **Refine**: "[Finding A] is actually a consequence of [Finding B].
  The root cause is the same."

The debate assignment follows a round-robin pattern:
```
correctness-reviewer      →  critiques  →  invariant-adversary, serialization
cross-component-reviewer  →  critiques  →  correctness, caller-audit
invariant-adversary       →  critiques  →  correctness, cross-component
caller-audit              →  critiques  →  invariant-adversary, cross-component
performance-reviewer      →  critiques  →  api, cross-component
api-reviewer              →  critiques  →  correctness, performance
serialization-reviewer    →  critiques  →  correctness, invariant-adversary
test-reviewer             →  critiques  →  serialization, caller-audit
design-reviewer           →  critiques  →  cross-component, invariant-adversary
```

**Note:** The invariant-adversary and caller-audit agents are deliberately
cross-linked with correctness and cross-component because their findings
often reveal the same underlying bug from different angles (invariant
violation vs reachable bad input vs data-flow mismatch).

Each agent should:
1. Read the assigned agents' findings files
2. Send a critique message to each assigned agent
3. Respond to critiques received from other agents
4. Update their own findings file with any revisions (upgraded/downgraded severity,
   withdrawn findings, new findings inspired by others)

#### Step 4c: Team lead collects debate results
- Read all updated findings files
- Review the debate messages
- Build consensus: findings supported by 2+ agents → HIGH confidence
- Write consensus document with final severity classifications

### 5. Consensus Phase
Team lead synthesizes the debate into a consensus document:
- Cross-reference overlapping findings across agents
- Count agreement (majority = 2+ agents independently flagging or supporting)
- Note disagreements and how they were resolved
- Classify findings as HIGH/MEDIUM/LOW severity
- Write consensus document

### 6. Summary Phase
- Compile all validated findings ordered by severity
- Include detailed bug analysis (root cause, vulnerable code paths)
- Include suggested fixes
- Note which findings were debated and the outcome
- Output the complete review as your final response text (this becomes the PR
  comment verbatim), and also overwrite review-findings.md with the identical
  content as a recovery backstop

**Final report quality rules:**
- The final report must be CLEAN and POLISHED. No stream-of-consciousness,
  no "Wait, actually...", no "on closer re-examination". Those belong in
  the working notes, not the output.
- If a finding was raised during review but later disproven during debate
  or deeper analysis, REMOVE IT entirely from the final report. Do not
  include it with a retraction — just drop it.
- If a finding was downgraded (e.g., Critical → Suggestion), present it
  at the final severity only. Do not narrate the severity change.
- Each finding in the final report should read as a confident, verified
  conclusion — not a record of the analysis process.
- The reader should never see the reviewer arguing with itself.

**REQUIRED output structure (so the PR page stays scrollable):**

Your final response — which becomes the PR comment verbatim — (and the
identical contents of `review-findings.md`) MUST follow this exact structure.
The summary appears first so reviewers can see HIGH findings at a glance;
everything else is hidden behind a `<details>` block.

```markdown
## Summary

<!-- One or two sentences of overall assessment. -->

**High-severity findings (N):**
- **[file.cc:123]** One-line description of the issue. <!-- repeat per HIGH finding -->

<!-- If there are NO high-severity findings, write exactly: -->
<!-- _No high-severity findings._ -->

<details>
<summary>Full review (click to expand)</summary>

### Findings

#### :red_circle: HIGH

##### H1. <Title> — `file.cc:123`
- **Issue:** ...
- **Root cause:** ...
- **Suggested fix:** ...

#### :yellow_circle: MEDIUM
... (same structure: M1, M2, ...)

#### :green_circle: LOW / NIT
... (same structure: L1, L2, ...)

### Cross-Component Analysis
<!-- Execution-context table and assumption stress-test results. -->

### Positive Observations
<!-- Optional: good patterns, clever optimizations. -->

</details>
```

Rules for this structure:
- The top-level `## Summary` and the bullet list of HIGH findings MUST stay
  outside the `<details>` block — they are always visible.
- Every detail (per-finding root cause, fix, debate outcomes, cross-component
  analysis, positive observations) MUST live inside the `<details>` block.
- Do NOT nest a `<details>` inside another `<details>`.
- Do NOT add any text after the closing `</details>` — the comment-builder
  appends its own footer.
- Your final response MUST be the review itself in this structure — never a
  meta-summary like "the review has been written to review-findings.md". The
  PR comment is generated verbatim from your final response.
- If the review is partial (recovery path), still produce the summary block
  first, and put whatever was salvaged inside the `<details>` block.

## Review Checklist

### Context Phase (must be completed before agents spawn)
- [ ] Subsystem deep-read — read changed files AND surrounding subsystem
      in depth (logic, edge cases, data flows, locking, concurrency)
- [ ] Caller chain for every changed public/virtual method (3-5 levels up)
- [ ] Critical decision points where callers branch on changed behavior
- [ ] Callee chain with SIDE EFFECTS — downstream dependencies, new
      preconditions, AND mutations to shared state (section 2c)
- [ ] Sibling implementations — how does the standard version handle same scenarios?
- [ ] Invariants that callers rely on
- [ ] Related functionality — existing helpers, patterns, conventions
- [ ] Cross-component data consumers — for every data WRITTEN, find ALL readers
      and verify visibility rules match (section 2g)
- [ ] Alternative execution contexts verified (section 2h table)
- [ ] Assumption stress-test — for every design claim, list preconditions
      and construct counterexamples (section 2i)
- [ ] Multi-component interactions (compaction, recovery, snapshots, iterators)
- [ ] Configuration dependencies and unexpected option combinations
- [ ] Potential pitfalls from caller-chain analysis
- [ ] DB-mutex reachability checked through transitive callees; affected lock
      and unlock boundaries documented

### Review Phase (verified by agents)
- [ ] Database semantics preserved (snapshot isolation, key ordering)
- [ ] All error cases handled
- [ ] Thread-safe with correct synchronization
- [ ] No data races or deadlocks
- [ ] Appropriate test coverage (edge cases, failure modes, system-level)
- [ ] No unnecessary allocations or copies in hot paths
- [ ] Backwards compatible
- [ ] New APIs consistent with existing patterns and documented
- [ ] Code follows RocksDB style conventions
- [ ] Behavioral contracts with upstream callers preserved
- [ ] All callers enumerated with parameter range table
- [ ] Before/after CPU and I/O under the DB mutex compared, with call frequency,
      scaling, hidden callee work, and worst-case cost included
- [ ] No significant DB-mutex-held work regression; unavoidable work is highly
      optimized, bounded, and supported by measurements when material

## Output Structure
Your **final response text is the primary output** — it becomes the PR comment
verbatim. The files below are working artifacts and recovery backstops only;
they are NOT a substitute for outputting the full review in your final response.

Write these artifacts to the working directory root:
- `review-findings.md` — Recovery backstop: incremental findings appended after
  each phase, overwritten with the final synthesized review at the end. Used
  only if the turn limit interrupts the run.
- `context.md` — Codebase context (call chains, invariants)
- `findings-*.md` — Per-agent findings (design, correctness, cross-component,
  invariant-adversary, caller-audit, performance, api, serialization, tests)
- `consensus.md` — Cross-review consensus (post-debate)



## Team Structure
```
Team Lead (you)
│
├── Phase 2: Codebase Context (team lead or dedicated research agent)
│   └── context-researcher    (general-purpose agent)
│       ├── Trace caller chains (3-5 levels up)
│       ├── Trace callee chains (dependencies AND side effects)
│       ├── Trace data consumers (who reads what the change writes?)
│       └── Document invariants
│
├── Phase 3: Initial Review (parallel, run_in_background)
│   │  (all agents receive context.md in their prompt)
│   ├── design-reviewer            (general-purpose agent)
│   ├── correctness-reviewer       (general-purpose agent)
│   ├── cross-component-reviewer   (general-purpose agent)
│   ├── invariant-adversary        (general-purpose agent)  ← NEW
│   ├── caller-audit               (general-purpose agent)  ← NEW
│   ├── performance-reviewer       (general-purpose agent)
│   ├── api-reviewer               (general-purpose agent)
│   ├── serialization-reviewer     (general-purpose agent)
│   └── test-reviewer              (general-purpose agent)
│
├── Phase 4: Debate (agents message each other)
│   ├── correctness ↔ invariant-adversary, serialization
│   ├── cross-component ↔ correctness, caller-audit
│   ├── invariant-adversary ↔ correctness, cross-component
│   ├── caller-audit ↔ invariant-adversary, cross-component
│   ├── performance ↔ api, cross-component
│   ├── api ↔ correctness, performance
│   ├── serialization ↔ correctness, invariant-adversary
│   ├── test-coverage ↔ serialization, caller-audit
│   └── design ↔ cross-component, invariant-adversary
│
```


## Agent Communication Protocol

### During Initial Review (Phase 3)
- Each agent writes findings to its own file
- Each agent sends a summary message to team lead when done
- Agents do NOT communicate with each other yet

### During Debate (Phase 4)
- Team lead sends each agent a message with instructions to:
  1. Read the assigned agents' findings files
  2. Send critique messages to those agents
  3. Respond to incoming critiques
  4. Update their own findings file with revisions
- Each critique message should include:
  - Which finding they're addressing (e.g., "Correctness F1")
  - Whether they AGREE, DISAGREE, or want to REFINE
  - Their reasoning with code evidence
  - Suggested severity adjustment (if any)

### Message format example
```
To: correctness-reviewer
Re: Your Finding F1

AGREE/DISAGREE/REFINE - [reasoning with code evidence].
[Suggested severity adjustment if any.]
```

## Review Anti-Patterns

These recurring failure modes lead to missed bugs. Each is detailed in the
referenced section; this table is a quick-reference checklist.

| Anti-Pattern | Fix | Reference |
|---|---|---|
| Return-Value Tunnel Vision | Trace callee MUTATIONS, not just returns | Section 2c |
| Default-Configuration Bias | Enumerate all execution contexts | Section 2h |
| Assert-as-Proof | Try to BREAK every assert | Section 2i |
| Write-Path-Only Analysis | Trace data readers, not just writers | Section 2g |
| Confirmation-Seeking Research | Use adversarial prompts ("find where X fails") | Invariant Adversary agent |
| Data-Flow vs Control-Flow Confusion | Separate who CALLS from who READS the data | Section 2g |
