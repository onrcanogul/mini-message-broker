package minibroker.broker;

import minibroker.storage.Log;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Owns the (topic, partition) -> Log mapping.
 *
 * v0 simplification: only partition 0 exists. Any other partition is rejected
 * upstream with UNKNOWN_TOPIC_OR_PARTITION. Multi-partition is Part 6+ territory.
 */
public class LogManager {

    /** The only valid partition in v0. */
    public static final int ONLY_PARTITION = 0;

    private final Path dataDir;
    // Keyed by "topic-partition". computeIfAbsent makes creation atomic so two
    // connection threads can never open the same file as two Log objects.
    private final Map<String, Log> logs = new ConcurrentHashMap<>();

    public LogManager(Path dataDir) {
        this.dataDir = dataDir;
        discoverExistingLogs();
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
                        logs.put(dir.getFileName().toString(), new Log(dir.resolve("partition.log")));
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
                return new Log(dir.resolve("partition.log"));
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
