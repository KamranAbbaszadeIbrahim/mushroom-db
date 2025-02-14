package com.mushroomdb;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Server exposes the Store operations over a TCP socket.
 * Clients connect and send simple text–based commands:
 * <pre>
 *   PUT key value
 *   GET key
 *   RANGE startKey endKey
 *   BATCHPUT count
 *       (followed by “count” lines, each “key value”)
 *   DELETE key
 *   LISTKEYS
 *   MERGE
 * </pre>
 * Responses begin with "OK" for success, or "ERROR" / "NOT_FOUND".
 */
public class Server {
    private static final int PORT = 5000;
    private final Store store;
    private final ExecutorService executor;

    /**
     * Constructs a Server with the given Store and thread pool size.
     *
     * @param store          the Store instance.
     * @param threadPoolSize the number of threads in the pool.
     */
    public Server(Store store, int threadPoolSize) {
        this.store = store;
        this.executor = Executors.newFixedThreadPool(threadPoolSize);
    }

    /**
     * Starts the server and listens for incoming client connections.
     *
     * @throws IOException if an I/O error occurs.
     */
    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("INFO: Server listening on port " + PORT);
            while (!executor.isShutdown()) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(new ClientHandler(clientSocket, store));
            }
        }
    }

    /**
     * Shuts down the server by stopping the executor and closing the store.
     */
    public void shutdown() {
        System.out.println("INFO: Shutting down server...");
        executor.shutdownNow();
        try {
            store.close();
        } catch (IOException e) {
            System.err.println("ERROR closing store: " + e.getMessage());
        }
    }

    /**
     * ClientHandler processes one client connection.
     */
    private record ClientHandler(Socket socket, Store store) implements Runnable {

        @Override
        public void run() {
            try (
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                    PrintWriter out = new PrintWriter(
                            new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)
            ) {
                String line;
                while ((line = in.readLine()) != null) {
                    processCommand(line, in, out);
                }
            } catch (IOException e) {
                System.err.println("ERROR in ClientHandler: " + e.getMessage());
            } finally {
                try {
                    socket.close();
                } catch (IOException ex) {
                    System.err.println("ERROR closing client socket: " + ex.getMessage());
                }
            }
        }

        private void processCommand(String line, BufferedReader in, PrintWriter out) {
            String[] tokens = line.trim().split("\\s+");
            if (tokens.length == 0) {
                return;
            }
            String command = tokens[0].toUpperCase();
            try {
                switch (command) {
                    case "PUT": {
                        if (tokens.length < 3) {
                            out.println("ERROR Missing arguments for PUT");
                            break;
                        }
                        String key = tokens[1];
                        String value = line.substring(line.indexOf(key) + key.length()).trim();
                        store.put(key, value);
                        out.println("OK");
                        break;
                    }
                    case "GET": {
                        if (tokens.length != 2) {
                            out.println("ERROR GET requires a key");
                            break;
                        }
                        String key = tokens[1];
                        String value = store.read(key);
                        out.println(value == null ? "NOT_FOUND" : "OK " + value);
                        break;
                    }
                    case "LISTKEYS": {
                        List<String> keys = store.listKeys();
                        out.println("OK " + keys.size());
                        for (String key : keys) {
                            out.println(key);
                        }
                        break;
                    }
                    case "MERGE": {
                        store.merge();
                        out.println("OK MERGE COMPLETED");
                        break;
                    }
                    case "RANGE": {
                        if (tokens.length != 3) {
                            out.println("ERROR RANGE requires startKey and endKey");
                            break;
                        }
                        String startKey = tokens[1];
                        String endKey = tokens[2];
                        List<Map.Entry<String, String>> results = store.readKeyRange(startKey, endKey);
                        out.println("OK " + results.size());
                        for (Map.Entry<String, String> entry : results) {
                            out.println(entry.getKey() + " " + entry.getValue());
                        }
                        break;
                    }
                    case "BATCHPUT": {
                        if (tokens.length != 2) {
                            out.println("ERROR BATCHPUT requires count");
                            break;
                        }
                        int count;
                        try {
                            count = Integer.parseInt(tokens[1]);
                        } catch (NumberFormatException nfe) {
                            out.println("ERROR Invalid count for BATCHPUT");
                            break;
                        }
                        Map<String, String> batchEntries = new HashMap<>();
                        for (int i = 0; i < count; i++) {
                            String pairLine = in.readLine();
                            if (pairLine == null) {
                                break;
                            }
                            String[] pairTokens = pairLine.trim().split("\\s+", 2);
                            if (pairTokens.length < 2) {
                                out.println("ERROR Invalid key-value pair in BATCHPUT");
                                continue;
                            }
                            batchEntries.put(pairTokens[0], pairTokens[1]);
                        }
                        store.batchPut(batchEntries);
                        out.println("OK");
                        break;
                    }
                    case "DELETE": {
                        if (tokens.length != 2) {
                            out.println("ERROR DELETE requires a key");
                            break;
                        }
                        String key = tokens[1];
                        store.delete(key);
                        out.println("OK");
                        break;
                    }
                    default:
                        out.println("ERROR Unknown command");
                        break;
                }
            } catch (Exception ex) {
                out.println("ERROR " + ex.getMessage());
            }
        }
    }

    /**
     * Main method: starts the server.
     * Command-line arguments are optional.
     * For example: java com.mushroomdb.Server [dataDirectory] [maxFileSize]
     *
     * @param args command-line arguments.
     */
    public static void main(String[] args) {
        String dirPath = args.length > 0 ? args[0] : "data";
        long maxFileSize = args.length > 1 ? Long.parseLong(args[1]) : 256;
        try {
            final Store store = new Store(dirPath, true, maxFileSize);
            final Server server = new Server(store, 10);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("INFO: Shutdown hook triggered");
                server.shutdown();
            }));
            server.start();
        } catch (IOException e) {
            System.err.println("ERROR starting server: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
