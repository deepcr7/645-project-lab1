package buffermanager;

import java.io.File;


public class QueryExecutor {
        private static final String MOVIES_FILE = "imdb_movies.bin";
        private static final String WORKEDON_FILE = "imdb_workedon.bin";
        private static final String PEOPLE_FILE = "imdb_people.bin";
        private static final String TEMP_FILTERED_WORKEDON_FILE = new File(System.getProperty("user.dir"),
                        "imdb_temp_filtered_workedon.bin").getAbsolutePath();

        private final ExtendedBufferManager bufferManager;
        private final String startRange;
        private final String endRange;
        private final int bufferSize;

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
        }

        /**
         * Executes the query and writes the results to standard output.
         */
        public void execute() {
                // Delete any existing temporary file before starting
                File tempFile = new File(TEMP_FILTERED_WORKEDON_FILE);
                if (tempFile.exists()) {
                        tempFile.delete();
                        System.err.println("Deleted existing temporary file before query execution");
                }

                // Create operators for the base tables
                ScanOperator moviesScan = new ScanOperator(
                                bufferManager,
                                MOVIES_FILE,
                                PageFactory.TableType.MOVIES,
                                new String[] { "Movies.movieId", "Movies.title" });

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
                SelectionOperator moviesTitleSelection = new SelectionOperator(
                                moviesScan,
                                new RangePredicate("Movies.title", startRange, endRange));

                SelectionOperator workedOnCategorySelection = new SelectionOperator(
                                workedOnScan,
                                new EqualityPredicate("WorkedOn.category", "director") {
                                        @Override
                                        public boolean test(Tuple tuple) {
                                                String category = tuple.getValue("WorkedOn.category");
                                                if (category != null) {
                                                        category = category.trim().toLowerCase();
                                                        return category.equals("director") ||
                                                                        category.equals("directors") ||
                                                                        category.contains("direct");
                                                }
                                                return false;
                                        }
                                });
                ProjectionOperator moviesProjection = new ProjectionOperator(
                                moviesTitleSelection,
                                new String[] { "Movies.movieId", "Movies.title" },
                                new String[] { "Movies.movieId", "Movies.title" });

                ProjectionOperator workedOnProjection = new ProjectionOperator(
                                workedOnCategorySelection,
                                new String[] { "WorkedOn.movieId", "WorkedOn.personId" },
                                new String[] { "WorkedOn.movieId", "WorkedOn.personId", "WorkedOn.category" });

                // Materialize the WorkedOn projection
                workedOnProjection.setMaterialize(bufferManager, TEMP_FILTERED_WORKEDON_FILE);

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
                        finalProjection.open();

                        // Print the results in CSV format
                        Tuple result;
                        while ((result = finalProjection.next()) != null) {
                                System.out.println(result.toCsv());
                        }

                        // Close the operators
                        finalProjection.close();

                } catch (Exception e) {
                        System.err.println("Error executing query: " + e.getMessage());
                        e.printStackTrace();
                        throw new RuntimeException("Error executing query: " + e.getMessage(), e);
                } finally {
                        // Clean up the temporary file at the end
                        if (tempFile.exists()) {
                                boolean deleted = tempFile.delete();
                                System.err.println("Cleanup: deleted temporary file: " + deleted);
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

                // Delete any existing temporary file before starting
                File tempFile = new File(TEMP_FILTERED_WORKEDON_FILE);
                if (tempFile.exists()) {
                        tempFile.delete();
                }

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

                // Materialize the WorkedOn projection
                workedOnProjection.setMaterialize(ioCounter, TEMP_FILTERED_WORKEDON_FILE);

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
                        // Clean up the temporary file
                        if (tempFile.exists()) {
                                tempFile.delete();
                        }
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
}