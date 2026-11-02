Fixed flush and compaction failing with Corruption when an SST using the experimental trie index (`TrieIndexFactory`) has a data block whose separator is the empty user key.
