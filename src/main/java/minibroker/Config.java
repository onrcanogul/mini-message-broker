package minibroker;

import java.nio.file.Path;

/** Broker configuration. port 0 = let the OS pick a free port (handy for tests). */
public record Config(int port, Path dataDir) {
}
