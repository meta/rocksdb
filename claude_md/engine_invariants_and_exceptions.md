# Engine invariants and compatibility

RocksDB developers use this guide when a feature combination involves an
exception to an ordinary engine guarantee. The principles in
[CLAUDE.md](../CLAUDE.md#engine-invariants-and-exceptions) remain the default
for other designs and reviews.

A documented exception relaxes only the stated guarantee. This boundary applies
to both new and existing combinations. Combined behavior must stay within the
feature's documented consequences. An exception does not permit additional loss
of live data or corruption. Documenting such a failure does not make it
acceptable.

Before adding compatibility logic for a feature that relaxes an invariant:

1. **Identify the contract.** State the normal invariant the component relies
   on. Identify the exceptional feature and the exact guarantee its public
   contract relaxes. A known implementation bug is not a documented exception.
   If the contract is unclear, call out the uncertainty rather than assume a
   waiver.

2. **Determine the required support scope.** Use the requested scope, public
   contracts, and known application requirements. The fact that both features
   exist does not require every new combination to be supported. Preserve
   existing accepted configurations under the Behavioral Compatibility
   requirements in `CLAUDE.md`; do not treat them as new support decisions.

3. **Support required combinations.** Use the smallest suitable design. Document
   the workload assumptions and test the relevant interaction. Prefer supporting
   a required combination over rejecting it when the change is moderate. Keep
   extra work out of configurations that do not need it.

4. **Keep other combinations within their contracts.** If a combination is
   outside the required support scope, retain the normal invariant in the new
   component. Do not add complex state, synchronization, or recovery logic
   solely to accommodate the exception. Document known compatibility limits.
   If the combination would violate a guarantee the exceptional feature still
   promises, support it or prevent it from being enabled. Reuse an existing
   validation check or add a small check when appropriate. Documenting the
   failure alone is not sufficient. Apply Behavioral Compatibility before
   restricting any existing accepted configuration.

5. **Review the combined behavior.** Check the documented exceptions and the
   remaining guarantees separately. Verify that the design does not introduce
   additional live-data loss or corruption. Test required interactions, and
   validate checks that reject unsupported new combinations.
