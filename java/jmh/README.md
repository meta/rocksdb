# JMH Benchmarks for RocksJava

These are micro-benchmarks for RocksJava functionality, using [JMH (Java Microbenchmark Harness)](https://openjdk.java.net/projects/code-tools/jmh/).

## Compiling

**Note**: This uses the build of RocksDB set by the `rocksdbjni.version` property of the `pom.xml` file, which defaults to a SNAPSHOT of the current version. Build and install that SNAPSHOT of rocksdbjni first, or pass `-Drocksdbjni.version=<version>` to Maven to use another one.

For instance, this is how to install the OSX jar you just built for 11.13.0

```bash
$ mvn install:install-file -Dfile=./java/target/rocksdbjni-11.13.0-osx.jar -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=11.13.0-SNAPSHOT -Dpackaging=jar
```

```bash
$ mvn package
```

## Running
```bash
$ java -jar target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar
```

NOTE: you can append `-help` to the command above to see all of the JMH runtime options.
