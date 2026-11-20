Fix SIGILL in range locking (`RangeLockManager`) on RISC-V Linux 6.6 and later, where user-mode `rdcycle` traps by default; use `rdtime` instead.
