package minibroker.broker;

import java.nio.file.Path;

/**
 * Broker configuration. port 0 = let the OS pick a free port (handy for tests).
 * segmentBytes        = roll the active log segment once it grows past this size.
 * indexIntervalBytes  = write one sparse .index entry per this many log bytes.
 * retentionMs         = delete closed segments whose newest record is older than this (-1 = off).
 * retentionBytes      = keep total log size under this by deleting oldest closed segments (-1 = off).
 */
public record Config(int port, Path dataDir, long segmentBytes, int indexIntervalBytes,
                     long retentionMs, long retentionBytes) {

    public static final long DEFAULT_SEGMENT_BYTES = 1L << 30;   // 1 GiB
    public static final int DEFAULT_INDEX_INTERVAL_BYTES = 4096; // 4 KiB
    public static final long RETENTION_OFF = -1;

    public Config(int port, Path dataDir) {
        this(port, dataDir, DEFAULT_SEGMENT_BYTES, DEFAULT_INDEX_INTERVAL_BYTES, RETENTION_OFF, RETENTION_OFF);
    }

    public Config(int port, Path dataDir, long segmentBytes) {
        this(port, dataDir, segmentBytes, DEFAULT_INDEX_INTERVAL_BYTES, RETENTION_OFF, RETENTION_OFF);
    }

    public Config(int port, Path dataDir, long segmentBytes, int indexIntervalBytes) {
        this(port, dataDir, segmentBytes, indexIntervalBytes, RETENTION_OFF, RETENTION_OFF);
    }
}
