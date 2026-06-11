package minibroker;

import minibroker.broker.BrokerServer;
import minibroker.broker.Config;
import minibroker.broker.LogManager;
import minibroker.broker.RequestHandler;

import java.io.IOException;
import java.nio.file.Path;

/** Wires up Config -> LogManager -> RequestHandler -> BrokerServer and starts listening. */
public final class Main {

    public static void main(String[] args) throws IOException, InterruptedException {
        // Usage: java minibroker.Main [dataDir]   (port is fixed at 9092 in v0)
        Path dataDir = Path.of(args.length > 0 ? args[0] : "data");
        Config config = new Config(9092, dataDir);
        LogManager logManager = new LogManager(config.dataDir());
        RequestHandler handler = new RequestHandler(logManager);

        BrokerServer server = new BrokerServer(config, handler);
        server.start();
        System.out.println("MiniBroker listening on port " + server.port()
                + ", data dir = " + config.dataDir().toAbsolutePath());
        server.awaitTermination(); // keep the JVM alive while serving
    }
}
