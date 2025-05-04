package buffermanager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

/**
 * Utility for testing the performance of the query executor.
 * Measures I/O operations and compares with analytical cost estimates.
 */
public class PerformanceTestLab3 {
    private static final String MOVIES_FILE = "imdb_movies.bin";
    private static final String WORKEDON_FILE = "imdb_workedon.bin";
    private static final String PEOPLE_FILE = "imdb_people.bin";
    private static final String RESULTS_FILE = "query_performance.csv";
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

        // Create test cases
        TestCase[] testCases = {
                new TestCase("A", "C", "Small range at start"),
                new TestCase("E", "H", "Small range in middle"),
                new TestCase("S", "Z", "Large range at end"),
                new TestCase("A", "M", "Medium range first half"),
                new TestCase("N", "Z", "Medium range second half"),
                new TestCase("A", "Z", "Full range")
        };

        // Calculate table sizes for cost formula
        long moviesSize = new File(MOVIES_FILE).length() / PageImpl.PAGE_SIZE;
        long workedOnSize = new File(WORKEDON_FILE).length() / PageImpl.PAGE_SIZE;
        long peopleSize = new File(PEOPLE_FILE).length() / PageImpl.PAGE_SIZE;

        // Create results file header
        try (PrintWriter writer = new PrintWriter(new FileWriter(RESULTS_FILE))) {
            writer.println("Test Case,Start Range,End Range,Description,Measured I/O,Estimated I/O,Ratio,Selectivity");

            // Run each test case
            for (TestCase testCase : testCases) {
                System.out.println("Running test case: " + testCase.description);

                // Create query executor
                QueryExecutor queryExecutor = new QueryExecutor(
                        bufferManager, testCase.startRange, testCase.endRange, bufferSize);

                // Measure I/O operations
                long ioCount = queryExecutor.countIoOperations();

                // Calculate movie selectivity by scanning
                double selectivity = calculateSelectivity(
                        bufferManager, MOVIES_FILE, testCase.startRange, testCase.endRange);

                // Estimate I/O cost using analytical formula
                long estimatedIoCost = estimateIoCost(
                        moviesSize, workedOnSize, peopleSize, selectivity, bufferSize);

                // Calculate ratio of measured to estimated
                double ratio = (double) ioCount / estimatedIoCost;

                // Write results to file
                writer.printf("%s,%s,%s,%s,%d,%d,%.2f,%.4f%n",
                        testCase.id, testCase.startRange, testCase.endRange, testCase.description,
                        ioCount, estimatedIoCost, ratio, selectivity);

                System.out.printf("Results: Measured I/O = %d, Estimated I/O = %d, Ratio = %.2f, Selectivity = %.4f%n",
                        ioCount, estimatedIoCost, ratio, selectivity);
            }
        } catch (IOException e) {
            System.err.println("Error writing results file: " + e.getMessage());
        }

        System.out.println("Performance tests complete. Results written to " + RESULTS_FILE);
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

        return totalCount > 0 ? (double) selectedCount / totalCount : 0;
    }

    /**
     * Estimates the I/O cost of the query using an analytical formula.
     * The formula is based on the relational algebra expression and the buffer
     * size.
     * 
     * @param moviesSize        The size of the Movies table in pages
     * @param workedOnSize      The size of the WorkedOn table in pages
     * @param peopleSize        The size of the People table in pages
     * @param moviesSelectivity The selectivity of the range predicate on Movies
     * @param bufferSize        The size of the buffer pool
     * @return The estimated I/O cost
     */
    private static long estimateIoCost(
            long moviesSize, long workedOnSize, long peopleSize,
            double moviesSelectivity, int bufferSize) {
        // Approximate selectivity for the WorkedOn table (directors only)
        double workedOnSelectivity = 0.1; // Assume 10% of records are directors

        // Cost components

        // 1. Scanning Movies (full scan)
        long moviesScanCost = moviesSize;

        // 2. Scanning WorkedOn (full scan)
        long workedOnScanCost = workedOnSize;

        // 3. Materializing filtered WorkedOn
        long materializedWorkedOnSize = (long) Math.ceil(workedOnSize * workedOnSelectivity);
        long materializationCost = materializedWorkedOnSize;

        // 4. BNL join of Movies and materialized WorkedOn
        // Cost = M + ceiling(M / (B-2)) * W'
        long blockSize = (bufferSize - 2) / 2;
        long moviesBlocks = (long) Math.ceil((moviesSize * moviesSelectivity) / (double) blockSize);
        long moviesWorkedOnJoinCost = (long) (moviesSize * moviesSelectivity) +
                moviesBlocks * materializedWorkedOnSize;

        // 5. Scanning People (full scan)
        long peopleScanCost = peopleSize;

        // 6. BNL join of (Movies join WorkedOn) and People
        // Assuming the join result size is proportional to the smaller relation after
        // applying selectivity
        long intermediateResultSize = (long) Math.min(
                moviesSize * moviesSelectivity,
                workedOnSize * workedOnSelectivity);
        long intermediateBlocks = (long) Math.ceil(intermediateResultSize / (double) blockSize);
        long finalJoinCost = intermediateResultSize + intermediateBlocks * peopleSize;

        // Total cost
        return moviesScanCost + workedOnScanCost + materializationCost +
                moviesWorkedOnJoinCost + peopleScanCost + finalJoinCost;
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
}