# Manual (Local) Code Review — Lightweight Wrapper

## What this is

A resource-light, **single-reviewer** variant of `claude_md/ci_review_prompt.md`
for running a review by hand in a local Claude Code / Devmate session. It reuses
the full CI review *methodology* but drops the multi-agent orchestration and the
CI-only file-persistence machinery, to approximate the heavyweight result at a
fraction of the cost.

**Scope: LOCAL manual review only.**

## How to use

Read `claude_md/ci_review_prompt.md` for the complete methodology — the analysis
techniques (caller-chain, callee side-effects, cross-component data consumers,
alternative execution contexts, assumption stress-testing), the per-perspective
reviewer instructions, the review checklist, and the anti-patterns table.

Then apply that methodology with the OVERRIDES below. **Where this file
conflicts with `ci_review_prompt.md`, this file wins.**

## Overrides (this is what makes it lightweight)

### 1. Single reviewer — no team, no subagents
- Do NOT create a team, spawn subagents, or run parallel/background agents.
- Do NOT run the round-robin Debate phase or the Consensus phase.
- Instead, YOU personally apply each reviewer role from `ci_review_prompt.md`
  (Design, Correctness, Cross-Component & Adversarial, Invariant Adversary,
  Caller-Context, Performance, API/Compat, Serialization, Test Coverage) as
  sequential *lenses* in one pass. Spend a few focused minutes per lens. Skip
  lenses that plainly don't apply (e.g., Serialization when there is no on-disk
  or wire-format change) and note the skip in one line.

### 2. No intermediate files
- Do NOT write `context.md`, `findings-*.md`, `consensus.md`, or
  `review-findings.md`. Keep working notes in your reasoning, not on disk.
- Rationale: those files exist only so CI can recover partial results when an
  ephemeral runner hits the turn limit — a local session has neither problem.
  `ci_review_prompt.md` already outputs the review as its final response, so
  skipping the file loses nothing here.

### 3. Right-size the effort
- Do the Codebase Context phase, but proportionally: trace the 2-3 most
  load-bearing call chains and the highest-risk cross-component data consumers,
  rather than exhaustively enumerating every caller 3-5 levels up.
- Prioritize the highest-signal lenses for RocksDB: **Correctness**,
  **Cross-Component & Adversarial**, and **Assumption Stress-Testing**. These
  catch the bugs that matter most; give them the most attention.
- Escalation clause: if a thread looks dangerous, go deep on that specific
  thread even though the overall pass is modest. Modest is the default, not a cap.

### 4. Identify the review target locally
- Unlike CI, the diff is NOT pre-pasted into this prompt. Determine what to
  review from the user's request against the local worktree — e.g. the current
  commit (`git show`), uncommitted changes (`git diff` / `git diff --staged`),
  a named commit range, or a PR — and read the changed files plus their
  surrounding context directly from the repo. When in doubt, include all changes
  not yet upstream, as in
  `git diff "$(git merge-base upstream/main HEAD 2> /dev/null || git merge-base origin/main HEAD 2> /dev/null)"`
  plus any untracked files (`git status`) that seem relevant.

## Reminder

The goal is a thorough *single* reviewer, not a 9-agent debate: depth where it
matters, brevity everywhere else.
