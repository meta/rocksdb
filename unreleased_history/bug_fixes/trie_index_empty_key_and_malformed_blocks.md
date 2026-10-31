Fixed the trie index dropping the empty user key, which failed flushes or left the SST unreadable through the trie, and fixed crashes when reading malformed trie index blocks.
