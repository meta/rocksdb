Fixed a use-after-free in pessimistic `TransactionDB` (point and range lock managers) when a column family was dropped while other threads were acquiring or releasing key locks in any column family.
