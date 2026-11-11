package org.rocksdb.japigen;

import java.util.Set;

public record Config(String generatedJSONDir, String targetSrcDir, String targetPackage, String jextractClass, Set<String> noConstructor) {
}
