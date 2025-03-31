# B+ Tree Indexing System

## Team Members and Contributions

- **Deepesh Suranjandass** (dsuranjandass@umass.edu)

- **Ajith Krishna Kanduri** (akanduri@umass.edu)

- **Spoorthi Siri Malladi** (smalladi@umass.edu)

## Design Choices

The B+ tree indexing system was designed with the following key considerations:

1. **Node Structure**: Each B+ tree node is implemented with a configurable order parameter that determines the maximum number of keys per node. This allows for flexibility in balancing tree height versus node utilization.

2. **Buffer Management**: The system extends the base buffer manager to support multiple index files, with pinning and unpinning mechanisms to control memory usage.

3. **Serialization Format**: Each node is serialized into a fixed-size page buffer, with careful handling of variable-length string keys and associated record IDs.

4. **Bulk Loading**: For the movieId index, a bulk loading approach was implemented to optimize insertion of sorted data, reducing the overhead of repeated node splitting.

5. **First-Level Pinning**: The implementation supports pinning the first levels of the B+ tree in memory to improve query performance, especially for frequently accessed paths.

6. **Error Handling**: Comprehensive error detection and recovery mechanisms were implemented to handle issues like buffer overflow during serialization.

## Running Instructions

### Prerequisites

- Java JDK 11 or higher
- Maven 3.6 or higher
- At least 4GB of available RAM
- Sufficient disk space for index files (approximately 3x the size of the raw data file)

### Building and Testing the Project

# Build the project

```sh
mvn clean compile
```

## Steps to Run

```sh
  mvn clean package
  cd target
  java -jar buffer-manager-1.0-SNAPSHOT.jar
```

## Steps to Test

```sh
# Load the initial dataset
mvn exec:java -Dexec.mainClass="buffermanager.Main"

# Run the end to end test(correctness and performance)
mvn exec:java -Dexec.mainClass="buffermanager.Assignment2Main"

# Run the performance tests separately (if needed)
mvn exec:java -Dexec.mainClass="buffermanager.PerformanceTest"

# Run unit tests
mvn test
```
