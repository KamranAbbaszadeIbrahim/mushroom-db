<svg height="200" width="200" viewBox="0 0 95.41467 95" xmlns="http://www.w3.org/2000/svg">
	<path d="m81.544 23.107c-2.4576 0.56738-2.6033 8.4226-0.4885 17.583 2.1148 9.1602 5.6892 16.157 8.1468 15.589 2.4576-0.56738 4.0039-7.3591 1.8891-16.519-2.114-9.161-7.089-17.221-9.547-16.653zm-74.732 45.567c-4.3819-5.246-5.8125-12.508-5.8125-21.598 0-13.988 5.8444-25.628 15.361-34.294 8.271-7.5316 19.351-11.782 31.346-11.782 14.072 0 24.859 4.3296 33.422 14.307 7.1343 8.3123 13.286 19.887 13.286 31.769 0 8.6494-1.4001 15.199-5.3973 20.352-3.2806 4.2293-7.8582 7.4188-13.293 9.7161-0.06661 4.9925-0.69044 10.405-6.2288 13.534-4.8461 2.7376-15.619 3.3214-22.451 3.3214-8.8851 0-17.766 0.13762-22.867-4.1518-3.277-2.754-4.646-8.417-4.932-12.512-5.015-2.058-9.284-4.89-12.434-8.661zm40.687-16.192c12.382 0 22.42-10.038 22.42-22.42s-10.038-22.42-22.42-22.42-22.42 10.038-22.42 22.42 10.038 22.42 22.42 22.42zm-41.403 2.67c2.4363 0.65281 6.2527-6.2145 8.6859-15.295 2.4332-9.0808 2.5617-16.936 0.12543-17.589-2.436-0.654-6.9895 4.617-9.4227 13.698s-1.8244 18.533 0.6119 19.186zm19.399 31.665c4.4687 2.9331 13.044 3.4463 21.174 3.4463 9.0167 0 19.014-0.03174 23.665-3.9387 2.741-2.3027 2.9062-8.7784 2.9062-11.753 0-1.2379-0.32761-3.0819-1.2455-4.2369-1.9638-2.4711-5.7605-4.0166-8.7187-5.3973-4.6188-2.1557-8.1814-2.4911-15.569-2.4911-6.813 0-11.988 0.65606-16.815 2.4911-3.8707 1.4716-8.5761 3.7606-9.9642 6.2276-0.52197 0.92768 0 2.2567 0 3.4066 0 3.4705 1.159 10.008 4.5669 12.245zm12.04-4.027c-1.3758 0-2.4911-1.3883-2.4911-8.3035 0-6.9153 1.1153-8.3035 2.4911-8.3035s2.4911 1.3883 2.4911 8.3035c0 6.9153-1.1153 8.3035-2.4911 8.3035zm19.928 0c-1.3758 0-2.4911-1.3883-2.4911-8.3035 0-6.9153 1.1153-8.3035 2.4911-8.3035s2.4911 1.3883 2.4911 8.3035c0 6.9153-1.1153 8.3035-2.4911 8.3035z"/>
</svg>

# Persistent Key-Value Store - MUSHROOM

This project implements a persistent key–value store inspired by designs like Bitcask. The system stores data using an append–only log file and maintains an in-memory index for fast key lookups. In addition, the project includes features for file rotation, merge (compaction) with hint files for fast recovery, and a simple TCP server that exposes a text–based protocol for interacting with the store. A basic replication mechanism is provided via the Replicator class to forward write commands to replica nodes.

## Features

- **Persistent Storage**:  
  Data is stored persistently on disk using an append–only log file. When the active file grows beyond a configured size, it is rotated, and old files are merged to remove obsolete data.

- **In-Memory Index (Keydir)**:  
  The store maintains an in-memory index (using a `ConcurrentSkipListMap`) that maps each key to a metadata record (including file name, offset, and value length) for fast read operations.

- **File Rotation & Merge (Compaction)**:  
  When the active file reaches a maximum size, it is rotated (renamed) and a new active file is started. A merge process compacts all immutable (rotated) files into a new merged file and generates a corresponding hint file to speed up recovery.

- **Hint Files**:  
  Hint files store key–value metadata so that on startup the in-memory index can be quickly rebuilt without scanning entire log files.

- **Batch Operations**:  
  Support for batch PUT operations is available to efficiently write multiple key–value pairs.

- **Simple TCP Server**:  
  A TCP server is provided that listens for text–based commands. Supported commands include:
    - `PUT key value` – Stores a key–value pair.
    - `GET key` – Retrieves the value for a key.
    - `RANGE startKey endKey` – Returns all key–value pairs in the specified range.
    - `BATCHPUT count` – Followed by `count` lines of key–value pairs.
    - `DELETE key` – Deletes a key.
    - `LISTKEYS` – Lists all keys.
    - `MERGE` – Triggers a merge (compaction) of rotated files.

- **Replication & Failover**:  
  The Replicator class forwards write operations (PUT, DELETE, BATCHPUT) to a list of replica nodes over TCP. If a replica fails to respond with an "OK" message, the failure is logged.

## Implementation Details

### Store Class
The `Store` class handles all persistent storage functionality:
- **Append-Only Log**:  
  Each write operation appends a record to the active log file using the following structure:
    - For PUT records: `[int recordLength][byte RECORD_PUT][int keyLength][keyBytes][int valueLength][valueBytes]`
    - For DELETE records: `[int recordLength][byte RECORD_DELETE][int keyLength][keyBytes]`
- **In-Memory Index**:  
  A `ConcurrentSkipListMap` is used to map keys to a `Metadata` record containing the file name, offset, and value length.
- **Recovery**:  
  Upon startup, all log files in the data directory are "replayed." If a hint file is available for a log file, it is loaded to rebuild the index faster.
- **File Rotation and Merge**:  
  When the active file exceeds a specified maximum size, it is rotated. The `merge()` method compacts all immutable files into a single merged file, updates the in-memory index, and generates a new hint file.
- **Replication**:  
  If a `Replicator` is set, write operations (PUT, BATCHPUT, DELETE) are forwarded to replica nodes.

### Server Class
The `Server` class provides a TCP interface to interact with the store:
- **TCP Listener**:  
  Listens on a configurable port (default is 5000) for incoming client connections.
- **Thread Pool**:  
  Uses an `ExecutorService` to handle multiple client connections concurrently.
- **Supported Commands**:  
  The server accepts commands such as PUT, GET, RANGE, BATCHPUT, DELETE, LISTKEYS, and MERGE. Each command is parsed and forwarded to the appropriate method in the `Store` class.
- **Graceful Shutdown**:  
  A shutdown hook is added to gracefully shut down the thread pool and close the store when the application terminates.

### Replicator Class
The `Replicator` class is responsible for forwarding write commands to replica nodes:
- **Replica Nodes**:  
  Accepts a list of replica node addresses (as `InetSocketAddress` objects) and a connection timeout.
- **Forwarding Commands**:  
  For each write command (PUT, DELETE, BATCHPUT), a TCP connection is established to each replica. The command is sent and the response is checked. Any errors are logged.

## How to Build and Run

### Prerequisites
- Java Development Kit (JDK) 11 or higher.

### Building the Project
1. **Compile Manually**:  
   Open a terminal in the project directory and run:
   ```bash
   javac -d out src/com/mushroomdb/*.java
   ```
   Ensure your source files are in the appropriate package directory (e.g., src/com/mushroomdb/).
2. **Using a Build Tool (Optional)**:
   You may create a Maven or Gradle project if you prefer to use a build tool.
### Running the Server
1. **Basic Run**:
    From the project directory, run:
   ```bash
    java -cp out com.mushroomdb.Server
   ```
    By default, the server will use the data directory, a maximum file size of 256 bytes (for testing), and listen on port 5000.
2. **Command-Line Arguments**:
   You can specify:
- **Data directory path**
- **Maximum active file size (in bytes)**
- **(Optionally) Replica addresses (comma-separated in the form host:port)**
<br>Example
   ```bash
    java -cp out com.mushroomdb.Server data 1024 localhost:5001,localhost:5002
   ```
### Using the Server
- **Connecting via Telnet/Netcat**:
    Open a terminal and connect using:
    ```bash
    telnet localhost 5000
    ```
    or
    ```bash
    nc localhost 5000
    ```
- **Supported Commands**:
  - **PUT**:
  ```query
  PUT myKey myValue
  ```
  - **GET**:
  ```query
  GET myKey
  ```
  - **RANGE**:
  ```query
  RANGE a z
  ```
  - **BATCHPUT**:
  ```query
  BATCHPUT 2
  key1 value1
  key2 value2
  ```
  - **DELETE**:
  ```query
  DELETE myKey
  ```
  - **LISTKEYS**:
  ```query
  LISTKEYS
  ```
  - **MERGE**:
  ```query
  MERGE
  ```
- **Response Format**:
  Responses begin with "OK" for success. If an error occurs, you'll receive an "ERROR" message. For GET commands, if a key is not found, "NOT_FOUND" is returned.