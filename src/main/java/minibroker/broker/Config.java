package minibroker.broker;

import java.nio.file.Path;

/**
 * Broker configuration. port 0 = let the OS pick a free port (handy for tests).
 * segmentBytes        = roll the active log segment once it grows past this size.
 * indexIntervalBytes  = write one sparse .index entry per this many log bytes.
 */
public record Config(int port, Path dataDir, long segmentBytes, int indexIntervalBytes) {

    /** Default segment size when not specified (1 GiB, Kafka's default ballpark). */
    public static final long DEFAULT_SEGMENT_BYTES = 1L << 30;
    /** Default sparse-index interval (4 KiB, Kafka's default). */
    public static final int DEFAULT_INDEX_INTERVAL_BYTES = 4096;

    public Config(int port, Path dataDir) {
        this(port, dataDir, DEFAULT_SEGMENT_BYTES, DEFAULT_INDEX_INTERVAL_BYTES);
    }

    public Config(int port, Path dataDir, long segmentBytes) {
        this(port, dataDir, segmentBytes, DEFAULT_INDEX_INTERVAL_BYTES);
    }
}
