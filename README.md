# MiniBroker

Minimal message broker (Kafka-style append-only log) in plain Java — no frameworks, JDK + JUnit only.

**v0 scope:** single broker, single partition (`partition 0`), append-only log + TCP produce/fetch,
crash recovery (corrupt-tail truncation + log rediscovery on restart).

## Layout

Packages are split by responsibility; dependencies flow one way:
`storage` → `protocol` → `broker` / `client` → `Main`.

```
src/main/java/minibroker/
├── Main.java                 # entry point: wires everything up
├── storage/                  # Record, StoredRecord, Log   (append-only file, recovery)
├── protocol/                 # Protocol, ApiKey, ErrorCode, {Produce,Fetch}{Request,Response}
├── broker/                   # Config, LogManager, RequestHandler, BrokerServer
└── client/                   # BrokerClient (thin wire client)
src/test/java/minibroker/     # tests mirror the package they cover; AcceptanceTest at root
lib/                          # junit-platform-console-standalone.jar (downloaded, git-ignored)
scripts/                      # setup.sh, build.sh, test.sh, run.sh
```

## Quick start

```bash
scripts/setup.sh    # one-time: download the JUnit jar into lib/
scripts/build.sh    # compile main + tests into out/
scripts/test.sh     # build + run the full JUnit suite
scripts/run.sh      # build + start the broker (optional: scripts/run.sh /tmp/mb)
```

The broker listens on port 9092 and stores logs under `./data` (or the dir you pass).

<details>
<summary>Raw commands (no scripts)</summary>

```bash
JAR=lib/junit-platform-console-standalone.jar
javac -d out -cp "$JAR" $(find src -name '*.java')           # build
java -jar "$JAR" execute -cp out --scan-classpath            # test
java -cp out minibroker.Main /tmp/mb                         # run
```
</details>

## Try it from code

Use `BrokerClient` against a running broker:

```java
try (BrokerClient c = BrokerClient.connect("localhost", 9092)) {
    long base = c.produce("orders", List.of(
        new Record("k1".getBytes(), "hello".getBytes(), 0)));   // -> baseOffset
    BrokerClient.FetchResult res = c.fetch("orders", 0, 1 << 20);
    // res.records(), res.highWatermark()
}
```

## On-disk record format (big-endian, `partition.log`)

```
recLen(int32) | offset(int64) | timestamp(int64) |
keyLen(int32, -1=null) | key | valLen(int32) | value | crc32(int32)
```

`recLen` frames each record (skip / half-tail detection); `crc32` detects corruption.
On restart, the last partial/corrupt record is truncated; intact records are kept.
