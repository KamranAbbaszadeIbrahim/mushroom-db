package com.mushroomdb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Store implements a persistent key–value store.
 *
 * Store is implemented using an append–only log file. Each record written to disk has the following structure:
 * <pre>
 *   [int recordLength][byte recordType][int keyLength][keyBytes]
 *   For PUT records: [int valueLength][valueBytes]
 *   For DELETE records: no value bytes.
 * </pre>
 * The in–memory index (a sorted ConcurrentSkipListMap) maps keys to a Metadata record (file name, offset, and length).
 * On startup the files in the data directory are replayed so that the index reflects the most recent update for each key.
 * This version includes a merge (compaction) process, hint files, a listKeys() method, and support for replication.
 */
public class Store {
    private final File dataDir;
    private File activeFile;
    private RandomAccessFile writeFile;
    private Replicator replicator;
    private final ConcurrentSkipListMap<String, Metadata> index = new ConcurrentSkipListMap<>();
    private final Object writeLock = new Object();
    private final boolean syncOnWrite;
    private final long maxFileSize;
    private static final byte RECORD_PUT = 1;
    private static final byte RECORD_DELETE = 2;

    /**
     * Metadata holds the file name, file offset (to the value bytes), and the length of the value.
     */
    private record Metadata(String fileName, long valuePosition, int valueLength) { }

    /**
     * Creates or opens a Store that uses a directory for its data files.
     *
     * @param directoryPath Path to the directory (e.g., "data")
     * @param syncOnWrite   If true, every update is flushed to disk.
     * @param maxFileSize   Maximum size (in bytes) for the active file before rotation.
     * @throws IOException if an I/O error occurs.
     */
    public Store(String directoryPath, boolean syncOnWrite, long maxFileSize) throws IOException {
        this.dataDir = new File(directoryPath);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        this.syncOnWrite = syncOnWrite;
        this.maxFileSize = maxFileSize;
        activeFile = new File(dataDir, "active.log");
        writeFile = new RandomAccessFile(activeFile, "rw");
        writeFile.seek(writeFile.length());
        recover();
    }

    /**
     * Sets the Replicator to be used for forwarding write operations.
     *
     * @param replicator the replicator instance.
     */
    public void setReplicator(Replicator replicator) {
        this.replicator = replicator;
    }

    /**
     * Replays all log files in the directory to rebuild the in–memory index.
     * If a corresponding hint file exists for a log file, it is used instead.
     *
     * @throws IOException if an I/O error occurs.
     */
    private void recover() throws IOException {
        File[] files = dataDir.listFiles((dir, name) -> name.endsWith(".log"));
        if (files == null) {
            files = new File[0];
        }
        Arrays.sort(files, Comparator.comparing(File::getName));
        for (File file : files) {
            if ("active.log".equals(file.getName())) {
                continue;
            }
            File hintFile = new File(dataDir, file.getName().replace(".log", ".hint"));
            if (hintFile.exists()) {
                Map<String, Metadata> hints = loadHintFile(hintFile, file.getName());
                index.putAll(hints);
            } else {
                loadLogFile(file);
            }
        }
        loadLogFile(activeFile);
    }

    /**
     * Loads a log file (without a hint file) to update the in–memory index.
     *
     * @param file the log file.
     * @throws IOException if an I/O error occurs.
     */
    private void loadLogFile(File file) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long pointer = 0;
            while (pointer < raf.length()) {
                raf.seek(pointer);
                try {
                    int recordLength = raf.readInt();
                    byte recordType = raf.readByte();
                    if (recordType == RECORD_PUT) {
                        int keyLen = raf.readInt();
                        byte[] keyBytes = new byte[keyLen];
                        raf.readFully(keyBytes);
                        String key = new String(keyBytes, StandardCharsets.UTF_8);
                        int valueLen = raf.readInt();
                        long valuePosition = pointer + 4 + 1 + 4 + keyLen + 4;
                        raf.skipBytes(valueLen);
                        index.put(key, new Metadata(file.getName(), valuePosition, valueLen));
                    } else if (recordType == RECORD_DELETE) {
                        int keyLen = raf.readInt();
                        byte[] keyBytes = new byte[keyLen];
                        raf.readFully(keyBytes);
                        String key = new String(keyBytes, StandardCharsets.UTF_8);
                        index.remove(key);
                    } else {
                        System.err.println("Unknown record type in file " + file.getName() +
                                " at position " + pointer);
                        break;
                    }
                    pointer += 4 + recordLength;
                } catch (EOFException eof) {
                    break;
                }
            }
        }
    }

    /**
     * Loads metadata from a hint file.
     *
     * @param hintFile     The hint file.
     * @param dataFileName The name of the data file associated with this hint file.
     * @return A map of key to Metadata.
     * @throws IOException if an I/O error occurs.
     */
    private Map<String, Metadata> loadHintFile(File hintFile, String dataFileName) throws IOException {
        Map<String, Metadata> hintMap = new HashMap<>();
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(hintFile)))) {
            while (true) {
                try {
                    int keyLen = dis.readInt();
                    byte[] keyBytes = new byte[keyLen];
                    dis.readFully(keyBytes);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);
                    long valuePosition = dis.readLong();
                    int valueLength = dis.readInt();
                    hintMap.put(key, new Metadata(dataFileName, valuePosition, valueLength));
                } catch (EOFException eof) {
                    break;
                }
            }
        }
        return hintMap;
    }

    /**
     * Writes a hint file for a given merged data file.
     *
     * @param dataFileName The name of the data file for which to generate the hint file.
     * @param metadataMap  The metadata map for that file.
     * @throws IOException if an I/O error occurs.
     */
    private void generateHintFile(String dataFileName, Map<String, Metadata> metadataMap) throws IOException {
        String hintFileName = dataFileName.replace(".log", ".hint");
        File hintFile = new File(dataDir, hintFileName);
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(hintFile)))) {
            for (Map.Entry<String, Metadata> entry : metadataMap.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);
                dos.writeLong(entry.getValue().valuePosition());
                dos.writeInt(entry.getValue().valueLength());
            }
        }
    }

    /**
     * Puts a key–value pair into the store.
     *
     * @param key   the key.
     * @param value the value.
     * @throws IOException if an I/O error occurs.
     */
    public void put(String key, String value) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        int recordLength = 1 + 4 + keyBytes.length + 4 + valueBytes.length;
        synchronized (writeLock) {
            if (writeFile.getFilePointer() + 4 + recordLength > maxFileSize) {
                rotateActiveFile();
            }
            long recordStart = writeFile.getFilePointer();
            writeFile.writeInt(recordLength);
            writeFile.writeByte(RECORD_PUT);
            writeFile.writeInt(keyBytes.length);
            writeFile.write(keyBytes);
            writeFile.writeInt(valueBytes.length);
            writeFile.write(valueBytes);
            if (syncOnWrite) {
                writeFile.getFD().sync();
            }
            long valuePosition = recordStart + 4 + 1 + 4 + keyBytes.length + 4;
            index.put(key, new Metadata(activeFile.getName(), valuePosition, valueBytes.length));
            if (replicator != null) {
                replicator.replicatePut(key, value);
            }
        }
    }

    /**
     * BatchPut writes a collection of key–value pairs in one batch.
     *
     * @param entries a map of keys and values.
     * @throws IOException if an I/O error occurs.
     */
    public void batchPut(Map<String, String> entries) throws IOException {
        synchronized (writeLock) {
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
                int recordLength = 1 + 4 + keyBytes.length + 4 + valueBytes.length;
                if (writeFile.getFilePointer() + 4 + recordLength > maxFileSize) {
                    rotateActiveFile();
                }
                long recordStart = writeFile.getFilePointer();
                writeFile.writeInt(recordLength);
                writeFile.writeByte(RECORD_PUT);
                writeFile.writeInt(keyBytes.length);
                writeFile.write(keyBytes);
                writeFile.writeInt(valueBytes.length);
                writeFile.write(valueBytes);
                long valuePosition = recordStart + 4 + 1 + 4 + keyBytes.length + 4;
                index.put(key, new Metadata(activeFile.getName(), valuePosition, valueBytes.length));
            }
            if (syncOnWrite) {
                writeFile.getFD().sync();
            }
            if (replicator != null) {
                replicator.replicateBatchPut(entries);
            }
        }
    }

    /**
     * Deletes a key from the store.
     *
     * @param key the key to delete.
     * @throws IOException if an I/O error occurs.
     */
    public void delete(String key) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int recordLength = 1 + 4 + keyBytes.length;
        synchronized (writeLock) {
            if (writeFile.getFilePointer() + 4 + recordLength > maxFileSize) {
                rotateActiveFile();
            }
            writeFile.writeInt(recordLength);
            writeFile.writeByte(RECORD_DELETE);
            writeFile.writeInt(keyBytes.length);
            writeFile.write(keyBytes);
            if (syncOnWrite) {
                writeFile.getFD().sync();
            }
            index.remove(key);
            if (replicator != null) {
                replicator.replicateDelete(key);
            }
        }
    }

    /**
     * Reads the value for the given key.
     *
     * @param key the key to read.
     * @return the value as a String, or null if not found.
     * @throws IOException if an I/O error occurs.
     */
    public String read(String key) throws IOException {
        Metadata metadata = index.get(key);
        if (metadata == null) {
            return null;
        }
        File file = new File(dataDir, metadata.fileName);
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(metadata.valuePosition());
            byte[] valueBytes = new byte[metadata.valueLength()];
            raf.readFully(valueBytes);
            return new String(valueBytes, StandardCharsets.UTF_8);
        }
    }

    /**
     * Reads the key–value pairs for all keys in the range [startKey, endKey).
     *
     * @param startKey the start key (inclusive).
     * @param endKey the end key (exclusive).
     * @return a list of key–value pair entries.
     * @throws IOException if an I/O error occurs.
     */
    public List<Map.Entry<String, String>> readKeyRange(String startKey, String endKey) throws IOException {
        List<Map.Entry<String, String>> results = new ArrayList<>();
        SortedMap<String, Metadata> subMap = index.subMap(startKey, endKey);
        for (Map.Entry<String, Metadata> entry : subMap.entrySet()) {
            String key = entry.getKey();
            Metadata metadata = entry.getValue();
            File file = new File(dataDir, metadata.fileName);
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                raf.seek(metadata.valuePosition());
                byte[] valueBytes = new byte[metadata.valueLength()];
                raf.readFully(valueBytes);
                results.add(new AbstractMap.SimpleEntry<>(key, new String(valueBytes, StandardCharsets.UTF_8)));
            }
        }
        return results;
    }

    /**
     * Returns a list of all keys in the store.
     *
     * @return a list of keys.
     */
    public List<String> listKeys() {
        return new ArrayList<>(index.keySet());
    }

    /**
     * Closes the store and its file handles.
     *
     * @throws IOException if an I/O error occurs.
     */
    public void close() throws IOException {
        writeFile.close();
    }

    /**
     * Rotates the active file when it reaches the maximum size.
     *
     * @throws IOException if an I/O error occurs.
     */
    private void rotateActiveFile() throws IOException {
        writeFile.close();
        String newName = "data_" + System.currentTimeMillis() + ".log";
        File rotatedFile = new File(dataDir, newName);
        if (!activeFile.renameTo(rotatedFile)) {
            throw new IOException("Failed to rotate active file.");
        }
        activeFile = new File(dataDir, "active.log");
        writeFile = new RandomAccessFile(activeFile, "rw");
    }

    /**
     * Merges (compacts) all immutable data files (rotated files) into a new merged file.
     * Only the latest version of each key is written to the merged file.
     * After merging, the old files are deleted and the in–memory index is updated.
     * A hint file is generated for the merged file to speed up future recovery.
     *
     * @throws IOException if an I/O error occurs.
     */
    public void merge() throws IOException {
        synchronized (writeLock) {
            File[] files = dataDir.listFiles((dir, name) -> name.endsWith(".log") && !name.equals("active.log"));
            if (files == null || files.length == 0) {
                System.out.println("No files to merge.");
                return;
            }
            Arrays.sort(files, Comparator.comparing(File::getName));
            String mergedFileName = "merged_" + System.currentTimeMillis() + ".log";
            File mergedFile = new File(dataDir, mergedFileName);
            RandomAccessFile mergeRAF = new RandomAccessFile(mergedFile, "rw");
            long mergedOffset = 0;
            Map<String, Metadata> newMetadata = new HashMap<>();
            for (File file : files) {
                RandomAccessFile raf = new RandomAccessFile(file, "r");
                long pointer = 0;
                while (pointer < raf.length()) {
                    raf.seek(pointer);
                    try {
                        int recordLength = raf.readInt();
                        byte recordType = raf.readByte();
                        if (recordType == RECORD_PUT) {
                            int keyLen = raf.readInt();
                            byte[] keyBytes = new byte[keyLen];
                            raf.readFully(keyBytes);
                            String key = new String(keyBytes, StandardCharsets.UTF_8);
                            int valueLen = raf.readInt();
                            long valuePosition = pointer + 4 + 1 + 4 + keyLen + 4;
                            Metadata currentMeta = index.get(key);
                            if (currentMeta != null &&
                                    currentMeta.fileName().equals(file.getName()) &&
                                    currentMeta.valuePosition() == valuePosition &&
                                    currentMeta.valueLength() == valueLen) {
                                byte[] valueBytes = new byte[valueLen];
                                raf.seek(valuePosition);
                                raf.readFully(valueBytes);
                                int newRecordLength = 1 + 4 + keyLen + 4 + valueLen;
                                mergeRAF.seek(mergedOffset);
                                mergeRAF.writeInt(newRecordLength);
                                mergeRAF.writeByte(RECORD_PUT);
                                mergeRAF.writeInt(keyLen);
                                mergeRAF.write(keyBytes);
                                mergeRAF.writeInt(valueLen);
                                mergeRAF.write(valueBytes);
                                long newValuePosition = mergedOffset + 4 + 1 + 4 + keyLen + 4;
                                newMetadata.put(key, new Metadata(mergedFileName, newValuePosition, valueLen));
                                mergedOffset += 4 + newRecordLength;
                            }
                        } else if (recordType == RECORD_DELETE) {
                            int keyLen = raf.readInt();
                            raf.skipBytes(keyLen);
                        } else {
                            System.err.println("Unknown record type in file " + file.getName() +
                                    " at pointer " + pointer);
                            break;
                        }
                        pointer += 4 + recordLength;
                    } catch (EOFException eof) {
                        break;
                    }
                }
                raf.close();
            }
            mergeRAF.getFD().sync();
            mergeRAF.close();
            index.putAll(newMetadata);
            generateHintFile(mergedFileName, newMetadata);
            for (File file : files) {
                if (!file.delete()) {
                    System.err.println("Failed to delete merged file " + file.getName());
                }
                File hintFile = new File(dataDir, file.getName().replace(".log", ".hint"));
                if (hintFile.exists() && !hintFile.delete()) {
                    System.err.println("Failed to delete hint file " + hintFile.getName());
                }
            }
            System.out.println("Merge completed. New merged file: " + mergedFileName);
        }
    }
}
