# LAB 3 Query Execution System

## Team Members and Contributions

- **Deepesh Suranjandass** (dsuranjandass@umass.edu)
 - Designed and implemented the core query execution engine
 - Developed the Block Nested Loop Join operator
 - Implemented buffer management for block handling
 - Created the hash table implementation for join operations
 - Fixed critical bugs in the materialization pipeline
 - Optimized the query execution for range predicates

- **Ajith Krishna Kanduri** (akanduri@umass.edu)
 - Implemented the Projection operator with materialization capability
 - Developed the Selection operator and predicate handling
 - Created the ScanOperator for base table access
 - Added support for selective materialization based on query ranges
 - Implemented I/O counting mechanisms for performance evaluation
 - Contributed to the formula for analytical cost estimation

- **Spoorthi Siri Malladi** (smalladi@umass.edu)
 - Implemented the table schema definitions for Movies, WorkedOn and People
 - Created the custom page implementations for different table types
 - Developed data loading utilities for IMDB datasets
 - Implemented error handling and recovery mechanisms
 - Contributed to comparison testing with PostgreSQL
 - Created the visualization system for performance metrics

## Design Choices

The query execution system was designed with the following key considerations:

1. **Iterator-Based Operators**: All operators implement the iterator interface (open, next, close) enabling pull-based query execution as described in lectures.

2. **Materialization Strategy**: The WorkedOn projection is materialized into a temporary table as required, while other operators use pipelining for efficiency.

3. **Block Nested Loop Join**: The BNL implementation maintains blocks of pages from the outer relation in memory and builds a hash table for efficient join condition evaluation.

4. **Buffer Management**: The system extends our base buffer manager to support multiple files and temporary pages, with appropriate pinning/unpinning mechanisms.

5. **Hash Table Design**: Efficient hash table implementation for the BNL join to quickly locate matching tuples from the outer relation.

6. **I/O Cost Modeling**: Analytical model to estimate I/O operations based on table sizes, selectivity, and buffer capacity.

## Running Instructions

### Prerequisites

- Java JDK 11 or higher
- Maven 3.6 or higher
- At least 8GB of available RAM
- PostgreSQL 12 or higher (for comparison testing)
- IMDB dataset files (title.basics.tsv, name.basics.tsv, title.principals.tsv)

### Building the Project

```sh
# Clone the repository
git clone https://github.com/deepcr7/645-project-lab1.git
cd 645-project-lab1

# Build the project
mvn clean package

# Run the pre-processing step to load the data and create the database files
java -Xmx8g -cp target/classes buffermanager.PreProcess 10000

# Run a query with specified title range and buffer size
java -Xmx8g -cp target/classes buffermanager.RunQuery A B 10000

# Run the optimized query implementation (recommended for large datasets)
java -Xmx8g -cp target/classes buffermanager.OptimizedRunQuery A B 10000

# Run performance tests with various ranges and generate metrics
java -Xmx8g -cp target/classes buffermanager.PerformanceTestLab3 10000

# Compare results with PostgreSQL (after setting up PostgreSQL with the same data)
./compare_results.sh