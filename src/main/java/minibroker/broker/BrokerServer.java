package minibroker.broker;

import minibroker.protocol.FetchResponse;
import minibroker.protocol.ProduceResponse;
import minibroker.protocol.Protocol;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Transport layer: ServerSocket + accept loop, one thread per connection.
 * Each connection runs: readFrame -> decodeRequest -> handler -> encode -> writeFrame.
 */
public class BrokerServer implements AutoCloseable {

    private final RequestHandler handler;
    private final ServerSocket serverSocket;
    private final ExecutorService connections = Executors.newCachedThreadPool();
    private volatile boolean running = false;
    private Thread acceptThread;

    public BrokerServer(Config config, RequestHandler handler) throws IOException {
        this.handler = handler;
        this.serverSocket = new ServerSocket(config.port()); // port 0 -> OS picks a free port
    }

    /** The actual bound port (useful when config.port == 0). */
    public int port() {
        return serverSocket.getLocalPort();
    }

    public void start() {
        running = true;
        acceptThread = new Thread(this::acceptLoop, "broker-accept");
        acceptThread.setDaemon(true); // daemon so embedding/tests never hang on it
        acceptThread.start();
    }

    /** Blocks until the accept loop ends. Main uses this to keep the JVM alive. */
    public void awaitTermination() throws InterruptedException {
        if (acceptThread != null) acceptThread.join();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                connections.submit(() -> handleConnection(socket));
            } catch (IOException e) {
                break; // serverSocket closed (shutdown) -> leave the loop
            }
        }
    }

    private void handleConnection(Socket socket) {
        // Buffered streams persist across frames; readFrame/writeFrame wrap them per call.
        try (socket;
             InputStream in = new BufferedInputStream(socket.getInputStream());
             OutputStream out = new BufferedOutputStream(socket.getOutputStream())) {
            while (true) {
                byte[] payload;
                try {
                    payload = Protocol.readFrame(in);
                } catch (EOFException eof) {
                    break; // client closed the connection cleanly
                }
                Protocol.DecodedRequest dec = Protocol.decodeRequest(payload);
                Object respBody = handler.handle(dec);
                Protocol.writeFrame(out, encodeResponse(dec.correlationId(), respBody));
            }
        } catch (IOException e) {
            // Connection-level error: just end this thread; other clients are unaffected.
        }
    }

    private static byte[] encodeResponse(int correlationId, Object body) throws IOException {
        if (body instanceof ProduceResponse pr) return Protocol.encodeResponse(correlationId, pr);
        if (body instanceof FetchResponse fr) return Protocol.encodeResponse(correlationId, fr);
        throw new IllegalStateException("Unknown response type: " + body);
    }

    @Override
    public void close() throws IOException {
        running = false;
        serverSocket.close();      // unblocks accept()
        connections.shutdownNow(); // interrupt in-flight connection threads
    }
}
