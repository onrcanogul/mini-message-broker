# MiniBroker

Minimal message broker (Kafka-style append-only log) in plain Java — no frameworks, JDK + JUnit only.

**v0 scope:** single broker, single partition (`partition 0`), append-only log + TCP produce/fetch,
crash recovery (corrupt-tail truncation + log rediscovery on restart).

## Layout

```
src/main/java/minibroker/   # Record, StoredRecord, Log, Protocol, LogManager,
                            # RequestHandler, BrokerServer, BrokerClient, Main, ...
src/test/java/minibroker/   # LogTest, LogRecoveryTest, ProtocolTest,
                            # BrokerServerTest, AcceptanceTest
lib/                        # junit-platform-console-standalone.jar
```

## Build

```bash
JAR=lib/junit-platform-console-standalone.jar
javac -d out $(find src -name '*.java') -cp "$JAR"
```

## Test

```bash
java -jar lib/junit-platform-console-standalone.jar execute -cp out --scan-classpath
```

## Run the broker

```bash
# Listens on port 9092; stores logs under ./data (or the dir you pass)
java -cp out minibroker.Main           # data dir = ./data
java -cp out minibroker.Main /tmp/mb   # custom data dir
```

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
