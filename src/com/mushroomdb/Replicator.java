package com.mushroomdb;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Replicator class forwards write commands to a list of replica nodes over TCP.
 * It supports replication for PUT, DELETE, and BATCHPUT operations.
 *
 * <p>Each command is sent to all provided replica nodes. If a replica fails to respond with "OK",
 * an error is logged.</p>
 */
public class Replicator {

    private final List<InetSocketAddress> replicaNodes;
    private final int timeoutMillis;

    /**
     * Constructs a Replicator with a list of replica node addresses and a connection timeout.
     *
     * @param replicaNodes  a list of {@link InetSocketAddress} representing the replica nodes.
     * @param timeoutMillis the connection timeout in milliseconds.
     */
    public Replicator(List<InetSocketAddress> replicaNodes, int timeoutMillis) {
        this.replicaNodes = new ArrayList<>(replicaNodes);
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * Replicates a PUT command for the given key and value to all replica nodes.
     *
     * @param key   the key to be stored.
     * @param value the value associated with the key.
     */
    public void replicatePut(String key, String value) {
        String command = "PUT " + key + " " + value;
        replicateCommand(command);
    }

    /**
     * Replicates a DELETE command for the given key to all replica nodes.
     *
     * @param key the key to be deleted.
     */
    public void replicateDelete(String key) {
        String command = "DELETE " + key;
        replicateCommand(command);
    }

    /**
     * Replicates a BATCHPUT command for the given entries to all replica nodes.
     *
     * @param entries a map containing key-value pairs to be stored.
     */
    public void replicateBatchPut(Map<String, String> entries) {
        StringBuilder commandBuilder = new StringBuilder("BATCHPUT " + entries.size());
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            commandBuilder.append("\n")
                    .append(entry.getKey())
                    .append(" ")
                    .append(entry.getValue());
        }
        replicateCommand(commandBuilder.toString());
    }

    /**
     * Sends the specified command to all replica nodes.
     *
     * @param command the command to be replicated.
     */
    private void replicateCommand(String command) {
        for (InetSocketAddress address : replicaNodes) {
            try (Socket socket = new Socket()) {
                socket.connect(address, timeoutMillis);
                PrintWriter out = new PrintWriter(
                        new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                out.println(command);
                String response = in.readLine();
                if (response == null || !response.startsWith("OK")) {
                    System.err.println("Replication to " + address + " failed: " + response);
                }
            } catch (IOException e) {
                System.err.println("Replication to " + address + " failed: " + e.getMessage());
            }
        }
    }
}
