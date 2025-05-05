package buffermanager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility for testing the performance of the query executor.
 * Measures I/O operations and compares with analytical cost estimates.
 */
public class PerformanceTestLab3 {
    private static final String MOVIES_FILE = "imdb_movies.bin";
    private static final String WORKEDON_FILE = "imdb_workedon.bin";
    private static final String PEOPLE_FILE = "imdb_people.bin";
    private static final String RESULTS_FILE = "query_performance.csv";
    private static final String CHART_DATA_FILE = "performance_chart_data.csv";
    private static final int DEFAULT_BUFFER_SIZE = 1000;

    /**
     * Main method for the performance test.
     * 
     * @param args Command line arguments (not used)
     */
    public static void main(String[] args) {
        System.out.println("Running performance tests...");

        int bufferSize = DEFAULT_BUFFER_SIZE;
        if (args.length > 0) {
            try {
                bufferSize = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid buffer size: " + args[0] + ". Using default: " + DEFAULT_BUFFER_SIZE);
                bufferSize = DEFAULT_BUFFER_SIZE;
            }
        }

        // Create buffer manager
        ExtendedBufferManager bufferManager = new ExtendedBufferManagerImpl(bufferSize);

        // Create test cases with different selectivity levels
        List<TestCase> testCases = new ArrayList<>();

        // Very small selectivity (~1%)
        testCases.add(new TestCase("A", "B", "Very small range"));

        // Small selectivity (~5%)
        testCases.add(new TestCase("A", "E", "Small range"));

        // Medium selectivity (~10%)
        testCases.add(new TestCase("E", "M", "Medium range"));

        // Large selectivity (~25%)
        testCases.add(new TestCase("A", "M", "Large range"));

        // Very large selectivity (~50%)
        testCases.add(new TestCase("A", "S", "Very large range"));

        // Full range (100%)
        testCases.add(new TestCase("A", "Z", "Full range"));

        // Calculate table sizes for cost formula
        long moviesSize = new File(MOVIES_FILE).length() / PageImpl.PAGE_SIZE;
        long workedOnSize = new File(WORKEDON_FILE).length() / PageImpl.PAGE_SIZE;
        long peopleSize = new File(PEOPLE_FILE).length() / PageImpl.PAGE_SIZE;

        System.out.println("Table sizes (pages):");
        System.out.println("- Movies: " + moviesSize);
        System.out.println("- WorkedOn: " + workedOnSize);
        System.out.println("- People: " + peopleSize);

        // Create results file header
        try (PrintWriter writer = new PrintWriter(new FileWriter(RESULTS_FILE));
                PrintWriter chartWriter = new PrintWriter(new FileWriter(CHART_DATA_FILE))) {

            writer.println("Test Case,Start Range,End Range,Description,Measured I/O,Estimated I/O,Ratio,Selectivity");
            chartWriter.println("Selectivity,Measured I/O,Estimated I/O");

            // Run each test case
            for (TestCase testCase : testCases) {
                System.out.println("\nRunning test case: " + testCase.description);
                System.out.println("Range: [" + testCase.startRange + ", " + testCase.endRange + "]");

                // Create query executor
                IoCountingBufferManager ioCounter = new IoCountingBufferManager(bufferManager);
                QueryExecutor queryExecutor = new QueryExecutor(
                        ioCounter, testCase.startRange, testCase.endRange, bufferSize);

                // Measure I/O operations
                long startTime = System.currentTimeMillis();
                queryExecutor.execute();
                long endTime = System.currentTimeMillis();
                long ioCount = ioCounter.getIoCount();
                long executionTime = endTime - startTime;

                // Calculate movie selectivity by scanning
                double selectivity = calculateSelectivity(
                        bufferManager, MOVIES_FILE, testCase.startRange, testCase.endRange);

                // Calculate WorkedOn selectivity (percentage of directors)
                double workedOnSelectivity = calculateWorkedOnSelectivity(bufferManager, WORKEDON_FILE);

                // Estimate I/O cost using analytical formula
                long estimatedIoCost = estimateIoCost(
                        moviesSize, workedOnSize, peopleSize, selectivity, workedOnSelectivity, bufferSize);

                // Calculate ratio of measured to estimated
                double ratio = (double) ioCount / estimatedIoCost;

                // Write results to file
                writer.printf("%s,%s,%s,%s,%d,%d,%.2f,%.4f%n",
                        testCase.id, testCase.startRange, testCase.endRange, testCase.description,
                        ioCount, estimatedIoCost, ratio, selectivity);

                // Write data for chart
                chartWriter.printf("%.4f,%d,%d%n", selectivity, ioCount, estimatedIoCost);

                System.out.printf("Results: Measured I/O = %d, Estimated I/O = %d%n", ioCount, estimatedIoCost);
                System.out.printf("Ratio = %.2f, Selectivity = %.4f%n", ratio, selectivity);
                System.out.printf("Execution time: %d ms%n", executionTime);
            }
        } catch (IOException e) {
            System.err.println("Error writing results file: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("\nPerformance tests complete. Results written to " + RESULTS_FILE);
    }

    /**
     * Calculates the selectivity of a range predicate on the Movies table.
     * 
     * @param bufferManager The buffer manager
     * @param filename      The name of the Movies table file
     * @param startRange    The lower bound for the title range
     * @param endRange      The upper bound for the title range
     * @return The selectivity (fraction of tuples that satisfy the predicate)
     */
    private static double calculateSelectivity(
            ExtendedBufferManager bufferManager, String filename, String startRange, String endRange) {
        System.out.println("Calculating selectivity for title range [" + startRange + ", " + endRange + "]...");

        // Create operators
        ScanOperator moviesScan = new ScanOperator(
                bufferManager,
                filename,
                PageFactory.TableType.MOVIES,
                new String[] { "Movies.movieId", "Movies.title" });

        SelectionOperator selection = new SelectionOperator(
                moviesScan,
                new RangePredicate("Movies.title", startRange, endRange));

        // Count tuples
        long totalCount = 0;
        long selectedCount = 0;

        // Count total movies
        moviesScan.open();
        while (moviesScan.next() != null) {
            totalCount++;
        }
        moviesScan.close();

        // Count selected movies
        selection.open();
        while (selection.next() != null) {
            selectedCount++;
        }
        selection.close();

        double selectivity = totalCount > 0 ? (double) selectedCount / totalCount : 0;
        System.out.printf("Selectivity: %.4f (%d out of %d records)%n",
                selectivity, selectedCount, totalCount);

        return selectivity;
    }

    /**
     * Calculates the selectivity of the category="director" predicate on the
     * WorkedOn table.
     * 
     * @param bufferManager The buffer manager
     * @param filename      The name of the WorkedOn table file
     * @return The selectivity (fraction of tuples that satisfy the predicate)
     */
    private static double calculateWorkedOnSelectivity(
            ExtendedBufferManager bufferManager, String filename) {
        System.out.println("Calculating selectivity for WorkedOn category='director'...");

        // Create operators
        ScanOperator workedOnScan = new ScanOperator(
                bufferManager,
                filename,
                PageFactory.TableType.WORKEDON,
                new String[] { "WorkedOn.movieId", "WorkedOn.personId", "WorkedOn.category" });

        SelectionOperator selection = new SelectionOperator(
                workedOnScan,
                new EqualityPredicate("WorkedOn.category", "director"));

        // Count tuples
        long totalCount = 0;
        long directorCount = 0;

        // Count total worked on records
        workedOnScan.open();
        while (workedOnScan.next() != null) {
            totalCount++;
        }
        workedOnScan.close();

        // Count director records
        selection.open();
        while (selection.next() != null) {
            directorCount++;
        }
        selection.close();

        double selectivity = totalCount > 0 ? (double) directorCount / totalCount : 0;
        System.out.printf("WorkedOn director selectivity: %.4f (%d out of %d records)%n",
                selectivity, directorCount, totalCount);

        return selectivity;
    }

    /**
     * Estimates the I/O cost of the query using an analytical formula.
     * The formula is based on the relational algebra expression and the buffer
     * size.
     * 
     * @param moviesSize          The size of the Movies table in pages
     * @param workedOnSize        The size of the WorkedOn table in pages
     * @param peopleSize          The size of the People table in pages
     * @param moviesSelectivity   The selectivity of the range predicate on Movies
     * @param workedOnSelectivity The selectivity of the category predicate on
     *                            WorkedOn
     * @param bufferSize          The size of the buffer pool
     * @return The estimated I/O cost
     */
    private static long estimateIoCost(
            long moviesSize, long workedOnSize, long peopleSize,
            double moviesSelectivity, double workedOnSelectivity, int bufferSize) {

        System.out.println("Estimating I/O cost with formula...");

        // Cost components

        // 1. Scanning Movies (full scan)
        long moviesScanCost = moviesSize;
        System.out.println("1. Movies scan cost: " + moviesScanCost + " I/Os");

        // 2. Scanning WorkedOn (full scan)
        long workedOnScanCost = workedOnSize;
        System.out.println("2. WorkedOn scan cost: " + workedOnScanCost + " I/Os");

        // 3. Materializing filtered WorkedOn
        long materializedWorkedOnSize = (long) Math.ceil(workedOnSize * workedOnSelectivity);
        long materializationCost = materializedWorkedOnSize;
        System.out.println("3. WorkedOn materialization cost: " + materializationCost + " I/Os");

        // 4. BNL join of Movies and materialized WorkedOn
        // Cost = M + ceiling(M / (B-2)) * W'
        long blockSize = (bufferSize - 2) / 2;
        if (blockSize < 1)
            blockSize = 1;

        long selectedMoviesSize = (long) Math.ceil(moviesSize * moviesSelectivity);
        long moviesBlocks = (long) Math.ceil(selectedMoviesSize / (double) blockSize);

        long moviesWorkedOnJoinCost = selectedMoviesSize +
                moviesBlocks * materializedWorkedOnSize;

        System.out.println("4. Movies-WorkedOn join cost: " + moviesWorkedOnJoinCost + " I/Os");
        System.out.println("   - Selected Movies size: " + selectedMoviesSize + " pages");
        System.out.println("   - Block size: " + blockSize + " pages");
        System.out.println("   - Number of blocks: " + moviesBlocks);

        // 5. Scanning People (full scan)
        long peopleScanCost = peopleSize;
        System.out.println("5. People scan cost: " + peopleScanCost + " I/Os");

        // 6. BNL join of (Movies join WorkedOn) and People
        // Assuming the join result size is proportional to the smaller relation after
        // applying selectivity
        long intermediateResultSize = (long) Math.min(
                selectedMoviesSize,
                materializedWorkedOnSize);

        long intermediateBlocks = (long) Math.ceil(intermediateResultSize / (double) blockSize);
        long finalJoinCost = intermediateResultSize + intermediateBlocks * peopleSize;

        System.out.println("6. Final join cost: " + finalJoinCost + " I/Os");
        System.out.println("   - Intermediate result size: " + intermediateResultSize + " pages");
        System.out.println("   - Number of blocks: " + intermediateBlocks);

        // Total cost
        long totalCost = moviesScanCost + workedOnScanCost + materializationCost +
                moviesWorkedOnJoinCost + peopleScanCost + finalJoinCost;

        System.out.println("Total estimated I/O cost: " + totalCost + " I/Os");

        return totalCost;
    }

    /**
     * Class representing a test case.
     */
    private static class TestCase {
        final String id;
        final String startRange;
        final String endRange;
        final String description;

        public TestCase(String startRange, String endRange, String description) {
            this.id = startRange + "-" + endRange;
            this.startRange = startRange;
            this.endRange = endRange;
            this.description = description;
        }
    }

    /**
     * Buffer manager wrapper that counts I/O operations.
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