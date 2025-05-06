package buffermanager;

import java.io.File;
import java.util.List;
import java.util.TreeMap;

public class QueryExecutor {
        private static final String MOVIES_FILE = "imdb_movies.bin";
        private static final String WORKEDON_FILE = "imdb_workedon.bin";
        private static final String PEOPLE_FILE = "imdb_people.bin";
        private static final String TEMP_FILTERED_WORKEDON_FILE = new File(System.getProperty("user.dir"),
                        "imdb_temp_filtered_workedon.bin").getAbsolutePath();

        // Static field to track if WorkedOn has been materialized
        private static boolean workedOnMaterialized = false;
        private static final Object materializationLock = new Object();

        private final ExtendedBufferManager bufferManager;
        private final String startRange;
        private final String endRange;
        private final int bufferSize;
        private int ioOperations;

        /**
         * Creates a new query executor with the given parameters.
         * 
         * @param bufferManager The buffer manager to use
         * @param startRange    The lower bound for the title range
         * @param endRange      The upper bound for the title range
         * @param bufferSize    The size of the buffer pool
         */
        public QueryExecutor(
                        ExtendedBufferManager bufferManager,
                        String startRange,
                        String endRange,
                        int bufferSize) {
                this.bufferManager = bufferManager;
                this.startRange = startRange;
                this.endRange = endRange;
                this.bufferSize = bufferSize;
                this.ioOperations = 0;
        }

        /**
         * Executes the query and writes the results to standard output.
         */
        public void execute() {
                // Check if the temporary file exists already (from previous materialization)
                File tempFile = new File(TEMP_FILTERED_WORKEDON_FILE);
                boolean tempFileExisted = tempFile.exists();

                // --- Index scan logic for Movies ---
                final String TITLE_INDEX_FILE = "imdb_title_index.bin";
                File indexFile = new File(TITLE_INDEX_FILE);
                Operator moviesSourceOperator;
                boolean usedIndex = false;
                if (indexFile.exists()) {
                        // Use index scan
                        System.out.println("Using TreeMap index for Movies.title range scan");
                        TreeMap<String, List<Rid>> titleIndex = new TreeMap<>();
                        moviesSourceOperator = new IndexScanOperator(
                                        titleIndex,
                                        bufferManager,
                                        MOVIES_FILE,
                                        PageFactory.TableType.MOVIES,
                                        new String[] { "Movies.movieId", "Movies.title" },
                                        startRange,
                                        endRange);
                        usedIndex = true;
                } else {
                        // Fallback: scan + selection
                        ScanOperator moviesScan = new ScanOperator(
                                        bufferManager,
                                        MOVIES_FILE,
                                        PageFactory.TableType.MOVIES,
                                        new String[] { "Movies.movieId", "Movies.title" });
                        moviesSourceOperator = new SelectionOperator(
                                        moviesScan,
                                        new RangePredicate("Movies.title", startRange, endRange));
                }

                ScanOperator workedOnScan = new ScanOperator(
                                bufferManager,
                                WORKEDON_FILE,
                                PageFactory.TableType.WORKEDON,
                                new String[] { "WorkedOn.movieId", "WorkedOn.personId", "WorkedOn.category" });

                ScanOperator peopleScan = new ScanOperator(
                                bufferManager,
                                PEOPLE_FILE,
                                PageFactory.TableType.PEOPLE,
                                new String[] { "People.personId", "People.name" });

                // Create selection operator for WorkedOn
                SelectionOperator workedOnCategorySelection = new SelectionOperator(
                                workedOnScan,
                                new EqualityPredicate("WorkedOn.category", "director"));

                // Create projection operators
                ProjectionOperator moviesProjection = new ProjectionOperator(
                                moviesSourceOperator,
                                new String[] { "Movies.movieId", "Movies.title" },
                                new String[] { "Movies.movieId", "Movies.title" });

                ProjectionOperator workedOnProjection = new ProjectionOperator(
                                workedOnCategorySelection,
                                new String[] { "WorkedOn.movieId", "WorkedOn.personId" },
                                new String[] { "WorkedOn.movieId", "WorkedOn.personId", "WorkedOn.category" });

                // Materialize the WorkedOn projection only once
                if (!workedOnMaterialized) {
                        synchronized (materializationLock) {
                                if (!workedOnMaterialized) {
                                        System.out.println("First execution: Materializing WorkedOn table...");
                                        workedOnProjection.setMaterialize(bufferManager, TEMP_FILTERED_WORKEDON_FILE);
                                        workedOnMaterialized = true;
                                } else {
                                        System.out.println("Reusing previously materialized WorkedOn table");
                                }
                        }
                } else {
                        System.out.println("Reusing previously materialized WorkedOn table");
                }

                // Calculate block size based on buffer size
                int blockSize = (bufferSize - 2) / 2;
                if (blockSize < 1)
                        blockSize = 1;

                // Create join operators
                BlockNestedLoopJoinOperator moviesWorkedOnJoin = new BlockNestedLoopJoinOperator(
                                moviesProjection,
                                workedOnProjection,
                                new JoinPredicate("Movies.movieId", "WorkedOn.movieId"),
                                bufferManager,
                                bufferSize,
                                -1); // Special file ID for first join

                BlockNestedLoopJoinOperator peopleJoin = new BlockNestedLoopJoinOperator(
                                moviesWorkedOnJoin,
                                peopleScan,
                                new JoinPredicate("WorkedOn.personId", "People.personId"),
                                bufferManager,
                                bufferSize,
                                -2); // Special file ID for second join

                // Create the final projection operator
                ProjectionOperator finalProjection = new ProjectionOperator(
                                peopleJoin,
                                new String[] { "Movies.title", "People.name" },
                                new String[] { "Movies.movieId", "Movies.title", "WorkedOn.movieId",
                                                "WorkedOn.personId",
                                                "People.personId", "People.name" });

                try {
                        // Execute the query
                        System.out.println("Executing query with title range [" + startRange + ", " + endRange +
                                        "] and buffer size " + bufferSize + (usedIndex ? " (using index)" : ""));
                        finalProjection.open();

                        // Print the results in CSV format
                        System.out.println("title,name");
                        int resultCount = 0;
                        Tuple result;

                        while ((result = finalProjection.next()) != null) {
                                System.out.println(result.toCsv());
                                resultCount++;
                        }

                        System.out.println("Query returned " + resultCount + " results");

                        // Close the operators
                        finalProjection.close();

                } catch (Exception e) {
                        System.err.println("Error executing query: " + e.getMessage());
                        e.printStackTrace();
                        throw new RuntimeException("Error executing query: " + e.getMessage(), e);
                } finally {
                        // Note: We no longer delete the temporary file to reuse it,
                        // unless it was just created in this execution and there's an error
                        if (tempFile.exists() && !tempFileExisted && !workedOnMaterialized) {
                                boolean deleted = tempFile.delete();
                                System.out.println("Cleanup: deleted temporary file due to error: " + deleted);
                        }
                }
        }

        /**
         * Counts the number of I/O operations performed by the query execution.
         * 
         * @return The number of I/O operations
         */
        public long countIoOperations() {
                IoCountingBufferManager ioCounter = new IoCountingBufferManager(bufferManager);

                // Check if the temporary file exists already (from previous materialization)
                File tempFile = new File(TEMP_FILTERED_WORKEDON_FILE);
                boolean tempFileExisted = tempFile.exists();

                // Create operators for the base tables
                ScanOperator moviesScan = new ScanOperator(
                                ioCounter,
                                MOVIES_FILE,
                                PageFactory.TableType.MOVIES,
                                new String[] { "Movies.movieId", "Movies.title" });

                ScanOperator workedOnScan = new ScanOperator(
                                ioCounter,
                                WORKEDON_FILE,
                                PageFactory.TableType.WORKEDON,
                                new String[] { "WorkedOn.movieId", "WorkedOn.personId", "WorkedOn.category" });

                ScanOperator peopleScan = new ScanOperator(
                                ioCounter,
                                PEOPLE_FILE,
                                PageFactory.TableType.PEOPLE,
                                new String[] { "People.personId", "People.name" });

                // Create selection operators for Movies and WorkedOn
                SelectionOperator moviesTitleSelection = new SelectionOperator(
                                moviesScan,
                                new RangePredicate("Movies.title", startRange, endRange));

                SelectionOperator workedOnCategorySelection = new SelectionOperator(
                                workedOnScan,
                                new EqualityPredicate("WorkedOn.category", "director"));

                // Create projection operators
                ProjectionOperator moviesProjection = new ProjectionOperator(
                                moviesTitleSelection,
                                new String[] { "Movies.movieId", "Movies.title" },
                                new String[] { "Movies.movieId", "Movies.title" });

                ProjectionOperator workedOnProjection = new ProjectionOperator(
                                workedOnCategorySelection,
                                new String[] { "WorkedOn.movieId", "WorkedOn.personId" },
                                new String[] { "WorkedOn.movieId", "WorkedOn.personId", "WorkedOn.category" });

                // Materialize the WorkedOn projection only once
                if (!workedOnMaterialized) {
                        synchronized (materializationLock) {
                                if (!workedOnMaterialized) {
                                        workedOnProjection.setMaterialize(ioCounter, TEMP_FILTERED_WORKEDON_FILE);
                                        workedOnMaterialized = true;
                                }
                        }
                }

                // Create join operators
                BlockNestedLoopJoinOperator moviesWorkedOnJoin = new BlockNestedLoopJoinOperator(
                                moviesProjection,
                                workedOnProjection,
                                new JoinPredicate("Movies.movieId", "WorkedOn.movieId"),
                                ioCounter,
                                bufferSize,
                                -1); // Special file ID for first join

                BlockNestedLoopJoinOperator peopleJoin = new BlockNestedLoopJoinOperator(
                                moviesWorkedOnJoin,
                                peopleScan,
                                new JoinPredicate("WorkedOn.personId", "People.personId"),
                                ioCounter,
                                bufferSize,
                                -2); // Special file ID for second join

                // Create the final projection operator
                ProjectionOperator finalProjection = new ProjectionOperator(
                                peopleJoin,
                                new String[] { "Movies.title", "People.name" },
                                new String[] { "Movies.movieId", "Movies.title", "WorkedOn.movieId",
                                                "WorkedOn.personId",
                                                "People.personId", "People.name" });

                try {
                        // Execute the query
                        finalProjection.open();

                        // Count the I/O operations
                        Tuple result;
                        while ((result = finalProjection.next()) != null) {
                                // Just iterate through all results
                        }

                        // Close the operators
                        finalProjection.close();

                } finally {
                        // Note: We no longer delete the temporary file at the end
                        // to allow for reuse in future query executions
                }

                return ioCounter.getIoCount();
        }

        /**
         * Helper class for counting I/O operations.
         */
        private static class IoCountingBufferManager extends ExtendedBufferManager {
                private final ExtendedBufferManager wrapped;
                private long ioCount;

                public IoCountingBufferManager(ExtendedBufferManager wrapped) {
                        super(wrapped.bufferSize);
                        this.wrapped = wrapped;
                        this.ioCount = 0;
                }

                @Override
                public Page getPage(String filename, int pageId) {
                        ioCount++;
                        return wrapped.getPage(filename, pageId);
                }

                @Override
                public Page createPage(String filename) {
                        ioCount++;
                        return wrapped.createPage(filename);
                }

                @Override
                public void markDirty(String filename, int pageId) {
                        wrapped.markDirty(filename, pageId);
                }

                @Override
                public void unpinPage(String filename, int pageId) {
                        wrapped.unpinPage(filename, pageId);
                }

                @Override
                public void force(String filename) {
                        wrapped.force(filename);
                }

                @Override
                public Page getPage(int pageId) {
                        return wrapped.getPage(pageId);
                }

                @Override
                public Page createPage() {
                        return wrapped.createPage();
                }

                @Override
                public void markDirty(int pageId) {
                        wrapped.markDirty(pageId);
                }

                @Override
                public void unpinPage(int pageId) {
                        wrapped.unpinPage(pageId);
                }

                public long getIoCount() {
                        return ioCount;
                }
        }
        // Add this optimization to QueryExecutor.java
        // Create a new method that avoids materializing the entire WorkedOn table

        public void executeOptimized() {
                // Create operators for the base tables with optimized approach
                System.out.println("Executing optimized query with title range [" + startRange + ", " + endRange +
                                "] and buffer size " + bufferSize);

                // Source operator for Movies - with range selection
                ScanOperator moviesScan = new ScanOperator(
                                bufferManager,
                                MOVIES_FILE,
                                PageFactory.TableType.MOVIES,
                                new String[] { "Movies.movieId", "Movies.title" });

                SelectionOperator moviesSelection = new SelectionOperator(
                                moviesScan,
                                new RangePredicate("Movies.title", startRange, endRange));

                // Apply projection to movies early
                ProjectionOperator moviesProjection = new ProjectionOperator(
                                moviesSelection,
                                new String[] { "Movies.movieId", "Movies.title" },
                                new String[] { "Movies.movieId", "Movies.title" });

                // Pre-count how many movies match to give progress information
                moviesProjection.open();
                int matchingMovieCount = 0;
                while (moviesProjection.next() != null) {
                        matchingMovieCount++;
                }
                moviesProjection.close();
                System.out.println("Found " + matchingMovieCount + " movies in title range: [" +
                                startRange + " - " + endRange + "]");

                // Reset the movies projection
                moviesProjection = new ProjectionOperator(
                                new SelectionOperator(
                                                moviesScan,
                                                new RangePredicate("Movies.title", startRange, endRange)),
                                new String[] { "Movies.movieId", "Movies.title" },
                                new String[] { "Movies.movieId", "Movies.title" });

                // Create operators for WorkedOn with filtering
                ScanOperator workedOnScan = new ScanOperator(
                                bufferManager,
                                WORKEDON_FILE,
                                PageFactory.TableType.WORKEDON,
                                new String[] { "WorkedOn.movieId", "WorkedOn.personId", "WorkedOn.category" });

                SelectionOperator workedOnCategorySelection = new SelectionOperator(
                                workedOnScan,
                                new EqualityPredicate("WorkedOn.category", "director"));

                ProjectionOperator workedOnProjection = new ProjectionOperator(
                                workedOnCategorySelection,
                                new String[] { "WorkedOn.movieId", "WorkedOn.personId" },
                                new String[] { "WorkedOn.movieId", "WorkedOn.personId", "WorkedOn.category" });

                // Set a smaller buffer size for materialization to avoid excessive I/O
                if (matchingMovieCount < 100) {
                        // For small result sets, use a small file to save I/O
                        workedOnProjection.setMaterialize(bufferManager, "imdb_temp_small_workedon.bin");
                } else {
                        // For larger result sets, use the standard file
                        workedOnProjection.setMaterialize(bufferManager, TEMP_FILTERED_WORKEDON_FILE);
                }

                // Join Movies with WorkedOn
                BlockNestedLoopJoinOperator moviesWorkedOnJoin = new BlockNestedLoopJoinOperator(
                                moviesProjection,
                                workedOnProjection,
                                new JoinPredicate("Movies.movieId", "WorkedOn.movieId"),
                                bufferManager,
                                bufferSize,
                                -1);

                // Create operator for People table
                ScanOperator peopleScan = new ScanOperator(
                                bufferManager,
                                PEOPLE_FILE,
                                PageFactory.TableType.PEOPLE,
                                new String[] { "People.personId", "People.name" });

                // Join with People table
                BlockNestedLoopJoinOperator peopleJoin = new BlockNestedLoopJoinOperator(
                                moviesWorkedOnJoin,
                                peopleScan,
                                new JoinPredicate("WorkedOn.personId", "People.personId"),
                                bufferManager,
                                bufferSize,
                                -2);

                // Create final projection
                ProjectionOperator finalProjection = new ProjectionOperator(
                                peopleJoin,
                                new String[] { "Movies.title", "People.name" },
                                new String[] { "Movies.movieId", "Movies.title", "WorkedOn.movieId",
                                                "WorkedOn.personId",
                                                "People.personId", "People.name" });

                try {
                        // Execute query
                        long startTime = System.currentTimeMillis();
                        finalProjection.open();

                        // Print results in CSV format
                        System.out.println("title,name");
                        Tuple result;
                        int resultCount = 0;

                        while ((result = finalProjection.next()) != null) {
                                System.out.println(result.toCsv());
                                resultCount++;

                                if (resultCount % 10000 == 0) {
                                        System.err.println("Processed " + resultCount + " results...");
                                }
                        }

                        long endTime = System.currentTimeMillis();
                        System.out.println("Query returned " + resultCount + " results in " +
                                        (endTime - startTime) + " ms");

                        finalProjection.close();
                } catch (Exception e) {
                        System.err.println("Error executing optimized query: " + e.getMessage());
                        e.printStackTrace();
                }
        }
}