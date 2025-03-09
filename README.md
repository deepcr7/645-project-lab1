# CS645 - Lab 1: Buffer Manager Implementation

## Overview

This project implements a **Buffer Manager** for a database system as part of **CS645 - Lab 1**. The buffer manager efficiently manages pages in memory using an **LRU (Least Recently Used) replacement policy** and supports essential operations such as **pinning/unpinning pages, dirty page tracking, and buffer eviction**.

### Features:
- **Fixed-size buffer pool** with efficient memory management.
- **LRU Replacement Policy** for page eviction.
- **Page-based storage** with a structured format (4KB pages).
- **Random access support** for efficient data retrieval.
- **IMDB dataset processing** with structured row storage

## Steps to Run

```sh
  mvn clean package
  cd target
  java -jar buffer-manager-1.0-SNAPSHOT.jar
```

## Steps to Test

```sh
    mvn test # Run all tests
    mvn -Dtest=BufferManagerTest test # Run BufferManager Tests
    mvn -Dtest=PageImplTest test # Run Page Tests
    mvn exec:java -Dexec.mainClass="buffermanager.Main" # Run end to end tests
```


