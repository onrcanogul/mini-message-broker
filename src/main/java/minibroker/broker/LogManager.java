package minibroker.broker;

import minibroker.storage.Log;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Owns the (topic, partition) -> Log mapping.
 *
 * v0 simplification: only partition 0 exists. Any other partition is rejected
 * upstream with UNKNOWN_TOPIC_OR_PARTITION. Multi-partition is Part 6+ territory.
 */
public class LogManager implements AutoCloseable {

    /** The only valid partition in v0. */
    public static final int ONLY_PARTITION = 0;

    private final Path dataDir;
    private final long segmentBytes;
    private final int indexInterval;
    private final long retentionMs;
    private final long retentionBytes;
    // Keyed by "topic-partition". computeIfAbsent makes creation atomic so two
    // connection threads can never open the same file as two Log objects.
    private final Map<String, Log> logs = new ConcurrentHashMap<>();
    private ScheduledExecutorService retentionExecutor;

    /** Logs never roll (single segment), retention off. Keeps the old one-arg contract. */
    public LogManager(Path dataDir) {
        this(dataDir, Long.MAX_VALUE, Config.DEFAULT_INDEX_INTERVAL_BYTES,
                Config.RETENTION_OFF, Config.RETENTION_OFF);
    }

    public LogManager(Path dataDir, long segmentBytes) {
        this(dataDir, segmentBytes, Config.DEFAULT_INDEX_INTERVAL_BYTES,
                Config.RETENTION_OFF, Config.RETENTION_OFF);
    }

    public LogManager(Path dataDir, long segmentBytes, int indexInterval) {
        this(dataDir, segmentBytes, indexInterval, Config.RETENTION_OFF, Config.RETENTION_OFF);
    }

    public LogManager(Path dataDir, long segmentBytes, int indexInterval,
                      long retentionMs, long retentionBytes) {
        this.dataDir = dataDir;
        this.segmentBytes = segmentBytes;
        this.indexInterval = indexInterval;
        this.retentionMs = retentionMs;
        this.retentionBytes = retentionBytes;
        discoverExistingLogs();
    }

    /** Periodically run retention across all logs (no-op if no retention is configured). */
    public synchronized void startRetention(long periodSeconds) {
        if (retentionExecutor != null) return;
        if (retentionMs <= 0 && retentionBytes <= 0) return;
        retentionExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "retention");
            t.setDaemon(true);
            return t;
        });
        retentionExecutor.scheduleAtFixedRate(() -> {
            for (Log log : logs.values()) {
                try {
                    log.enforceRetention();
                } catch (IOException ignored) {
                    // a failure on one log shouldn't stop the others
                }
            }
        }, periodSeconds, periodSeconds, TimeUnit.SECONDS);
    }

    @Override
    public synchronized void close() {
        if (retentionExecutor != null) retentionExecutor.shutdownNow();
    }

    /**
     * On startup, rediscover logs already on disk so a restarted broker knows which
     * topics/partitions exist (each Log's own recovery then rebuilds its offsets).
     * Recovering a single log file (Part 2) is not enough — the broker must also
     * recover the SET of logs.
     */
    private void discoverExistingLogs() {
        if (!Files.isDirectory(dataDir)) return; // fresh broker, nothing to load
        try (var dirs = Files.list(dataDir)) {
            dirs.filter(Files::isDirectory).forEach(dir -> {
                try {
                    // A partition dir is one that holds at least one segment (.log) file.
                    if (hasSegment(dir)) {
                        // Directory name IS the map key (topic + "-" + partition).
                        // Log only needs the directory; the path's parent identifies it.
                        logs.put(dir.getFileName().toString(), new Log(dir.resolve("partition.log"),
                                segmentBytes, indexInterval, retentionMs, retentionBytes, System::currentTimeMillis));
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean hasSegment(Path dir) throws IOException {
        try (var entries = Files.list(dir)) {
            return entries.anyMatch(p -> p.getFileName().toString().endsWith(".log"));
        }
    }

    public boolean isValidPartition(int partition) {
        return partition == ONLY_PARTITION;
    }

    /** Returns the Log, creating it (and its directory) on first use. Thread-safe. */
    public Log getOrCreate(String topic, int partition) {
        String key = topic + "-" + partition;
        return logs.computeIfAbsent(key, k -> {
            try {
                Path dir = dataDir.resolve(key);
                Files.createDirectories(dir);
                return new Log(dir.resolve("partition.log"),
                        segmentBytes, indexInterval, retentionMs, retentionBytes, System::currentTimeMillis);
            } catch (IOException e) {
                // computeIfAbsent can't propagate a checked exception; wrap it.
                throw new UncheckedIOException(e);
            }
        });
    }

    /** Returns an existing Log, or null if this topic/partition was never produced to. */
    public Log get(String topic, int partition) {
        return logs.get(topic + "-" + partition);
    }
}
