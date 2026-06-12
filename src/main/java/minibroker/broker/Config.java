package minibroker.broker;

import java.nio.file.Path;

/**
 * Broker configuration. port 0 = let the OS pick a free port (handy for tests).
 * segmentBytes = roll the active log segment once it grows past this size.
 */
public record Config(int port, Path dataDir, long segmentBytes) {

    /** Default segment size when not specified (1 GiB, Kafka's default ballpark). */
    public static final long DEFAULT_SEGMENT_BYTES = 1L << 30;

    public Config(int port, Path dataDir) {
        this(port, dataDir, DEFAULT_SEGMENT_BYTES);
    }
}
