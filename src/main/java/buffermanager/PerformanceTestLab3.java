package buffermanager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

/**
 * Utility for testing the performance of the optimized query executor.
 * Measures execution time and I/O operations and compares with analytical cost
 * estimates.
 */
public class PerformanceTestLab3 {
    private static final String MOVIES_FILE = "imdb_movies.bin";
    private static final String WORKEDON_FILE = "imdb_workedon.bin";
    private static final String PEOPLE_FILE = "imdb_people.bin";
    private static final String RESULTS_FILE = "test_results_lab3/query_performance.csv";
    private static final String CHART_DATA_FILE = "test_results_lab3/performance_chart_data.csv";
    private static final int DEFAULT_BUFFER_SIZE = 10000;

    // Constants for I/O cost calculation
    private static double WORKEDON_DIRECTOR_SELECTIVITY = 0.02; // Calculated offline
    private static long MOVIES_SIZE_PAGES;
    private static long WORKEDON_SIZE_PAGES;
    private static long PEOPLE_SIZE_PAGES;

    /**
     * Main method for the performance test.
     * 
     * @param args Command line arguments (buffer size)
     */
    public static void main(String[] args) {
        System.out.println("Running optimized performance tests...");

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

        // Calculate table sizes in pages
        calculateTableSizes();

        System.out.println("Table sizes in pages:");
        System.out.println("Movies: " + MOVIES_SIZE_PAGES);
        System.out.println("WorkedOn: " + WORKEDON_SIZE_PAGES);
        System.out.println("People: " + PEOPLE_SIZE_PAGES);

        // Create test cases with different selectivity levels
        List<TestCase> testCases = new ArrayList<>();

        // Very small selectivity
        testCases.add(new TestCase("Z", "Z", "Very small range"));

        // Small selectivity
        testCases.add(new TestCase("X", "Y", "Small range"));

        // Medium selectivity
        testCases.add(new TestCase("S", "T", "Medium range"));

        // // Large selectivity
        testCases.add(new TestCase("M", "R", "Large range"));

        // // Very large selectivity
        testCases.add(new TestCase("A", "L", "Very large range"));

        // // Full range
        testCases.add(new TestCase("A", "Z", "Full range"));

        // Create directory for test results
        File resultsDir = new File("test_results_lab3");
        if (!resultsDir.exists()) {
            resultsDir.mkdir();
        }

        // Create results file header
        try (PrintWriter writer = new PrintWriter(new FileWriter(RESULTS_FILE));
                PrintWriter chartWriter = new PrintWriter(new FileWriter(CHART_DATA_FILE))) {

            writer.println(
                    "Test Case,Start Range,End Range,Description,Execution Time (ms),Measured I/O,Estimated I/O,Selectivity,Results Count");
            chartWriter.println("Selectivity,Measured I/O,Estimated I/O");

            // Run each test case
            for (TestCase testCase : testCases) {
                System.out.println("\nRunning test case: " + testCase.description);
                System.out.println("Range: [" + testCase.startRange + ", " + testCase.endRange + "]");

                // First, calculate selectivity by scanning movies table
                double selectivity = calculateSelectivity(bufferManager, testCase.startRange, testCase.endRange);
                testCase.selectivity = selectivity;

                // Calculate estimated I/O based on our formula
                long estimatedIO = calculateEstimatedIO(selectivity, bufferSize);
                testCase.estimatedIO = estimatedIO;
                System.out.println("Estimated I/O: " + estimatedIO);

                // Run the optimized query and capture results and measure I/O
                IoCountingBufferManager ioCounter = new IoCountingBufferManager(bufferManager);
                List<String> results = runOptimizedQuery(testCase.startRange, testCase.endRange, bufferSize, ioCounter);
                testCase.resultCount = results.size();
                testCase.measuredIO = ioCounter.getIoCount();

                System.out.printf("Results: Count = %d, Selectivity = %.4f%n",
                        testCase.resultCount, testCase.selectivity);
                System.out.printf("Execution time: %d ms, Measured I/O: %d, Estimated I/O: %d%n",
                        testCase.executionTime, testCase.measuredIO, testCase.estimatedIO);

                // Write results to files
                writer.printf("%s,%s,%s,%s,%d,%d,%d,%.4f,%d%n",
                        testCase.id, testCase.startRange, testCase.endRange, testCase.description,
                        testCase.executionTime, testCase.measuredIO, testCase.estimatedIO,
                        testCase.selectivity, testCase.resultCount);

                chartWriter.printf("%.4f,%d,%d%n",
                        testCase.selectivity, testCase.measuredIO, testCase.estimatedIO);
            }
        } catch (IOException e) {
            System.err.println("Error writing results file: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("\nPerformance tests complete. Results written to " + RESULTS_FILE);

        // Generate plots
        try {
            PlotGenerator.main(null);
            System.out.println("Performance plots generated successfully.");
        } catch (Exception e) {
            System.err.println("Error generating plots: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Calculate table sizes in pages.
     */
    private static void calculateTableSizes() {
        File moviesFile = new File(MOVIES_FILE);
        File workedOnFile = new File(WORKEDON_FILE);
        File peopleFile = new File(PEOPLE_FILE);

        MOVIES_SIZE_PAGES = moviesFile.length() / PageImpl.PAGE_SIZE;
        WORKEDON_SIZE_PAGES = workedOnFile.length() / PageImpl.PAGE_SIZE;
        PEOPLE_SIZE_PAGES = peopleFile.length() / PageImpl.PAGE_SIZE;
    }

    /**
     * Calculate estimated I/O cost based on our analytical formula.
     * 
     * Total I/O Cost = M + W + 2×W×SW + M×SM + ceiling(M×SM/((B-C)/2))×W×SW + P +
     * IR + ceiling(IR/((B-C)/2))×P
     * 
     * Where:
     * M, W, P: Sizes of Movies, WorkedOn, and People tables in pages
     * SM: Selectivity of title range predicate
     * SW: Selectivity of category='director' predicate (constant)
     * B: Buffer size in pages
     * C: Number of frames used for input/output (C=2)
     * IR: Intermediate result size = min(M×SM, W×SW)
     */
    private static long calculateEstimatedIO(double movieSelectivity, int bufferSize) {
        int C = 2; // Number of frames used for input/output
        double SW = WORKEDON_DIRECTOR_SELECTIVITY;

        // Block size for BNL join
        int blockSize = (bufferSize - C) / 2;
        if (blockSize < 1)
            blockSize = 1;

        // Cost components

        // 1. Scanning Movies table
        long moviesScanCost = MOVIES_SIZE_PAGES;

        // 2. Scanning WorkedOn table
        long workedOnScanCost = WORKEDON_SIZE_PAGES;

        // 3. Materializing filtered WorkedOn
        long materializedWorkedOnSize = (long) Math.ceil(WORKEDON_SIZE_PAGES * SW);
        long materializationCost = 2 * materializedWorkedOnSize; // Write + Read

        // 4. BNL join of Movies and materialized WorkedOn
        long selectedMoviesSize = (long) Math.ceil(MOVIES_SIZE_PAGES * movieSelectivity);
        long numBlocks = (long) Math.ceil((double) selectedMoviesSize / blockSize);
        long joinCost1 = selectedMoviesSize + (numBlocks * materializedWorkedOnSize);

        // 5. Scanning People table
        long peopleScanCost = PEOPLE_SIZE_PAGES;

        // 6. BNL join with People table
        long intermediateResultSize = Math.min(selectedMoviesSize, materializedWorkedOnSize);
        long numBlocks2 = (long) Math.ceil((double) intermediateResultSize / blockSize);
        long joinCost2 = intermediateResultSize + (numBlocks2 * PEOPLE_SIZE_PAGES);

        // Total cost
        return moviesScanCost + workedOnScanCost + materializationCost + joinCost1 + peopleScanCost + joinCost2;
    }

    /**
     * Runs the optimized query and returns the results.
     */
    private static List<String> runOptimizedQuery(String startRange, String endRange, int bufferSize,
            IoCountingBufferManager ioCounter) {
        List<String> results = new ArrayList<>();

        try {
            // Measure execution time
            long startTime = System.currentTimeMillis();

            // STEP 1: Find all movies in the range and collect their IDs
            ScanOperator moviesScan = new ScanOperator(
                    ioCounter,
                    MOVIES_FILE,
                    PageFactory.TableType.MOVIES,
                    new String[] { "Movies.movieId", "Movies.title" });

            SelectionOperator moviesSelection = new SelectionOperator(
                    moviesScan,
                    new RangePredicate("Movies.title", startRange, endRange));

            // First pass: count and collect movie IDs
            Set<String> matchingMovieIds = new HashSet<>();
            List<Tuple> matchingMovies = new ArrayList<>();

            moviesSelection.open();
            Tuple tuple;
            while ((tuple = moviesSelection.next()) != null) {
                matchingMovieIds.add(tuple.getValue("Movies.movieId"));
                matchingMovies.add(tuple);
            }
            moviesSelection.close();

            // STEP 2: Filter WorkedOn to only directors from matching movies
            // Filter WorkedOn by category = 'director' and matching movie IDs
            ScanOperator workedOnScan = new ScanOperator(
                    ioCounter,
                    WORKEDON_FILE,
                    PageFactory.TableType.WORKEDON,
                    new String[] { "WorkedOn.movieId", "WorkedOn.personId", "WorkedOn.category" });

            // Two-stage filter
            SelectionOperator directorFilter = new SelectionOperator(
                    workedOnScan,
                    new EqualityPredicate("WorkedOn.category", "director"));

            // Create a custom selection operator to filter by movie IDs
            SelectionOperator movieIdFilter = new SelectionOperator(
                    directorFilter,
                    new Predicate() {
                        @Override
                        public boolean test(Tuple tuple) {
                            String movieId = tuple.getValue("WorkedOn.movieId");
                            return matchingMovieIds.contains(movieId);
                        }
                    });

            // Collect filtered WorkedOn records
            List<Tuple> matchingWorkedOn = new ArrayList<>();
            Set<String> matchingPersonIds = new HashSet<>();

            movieIdFilter.open();
            while ((tuple = movieIdFilter.next()) != null) {
                matchingWorkedOn.add(tuple);
                matchingPersonIds.add(tuple.getValue("WorkedOn.personId"));
            }
            movieIdFilter.close();

            // STEP 3: Find people names for matching person IDs
            ScanOperator peopleScan = new ScanOperator(
                    ioCounter,
                    PEOPLE_FILE,
                    PageFactory.TableType.PEOPLE,
                    new String[] { "People.personId", "People.name" });

            // Create a custom selection operator to filter by person IDs
            SelectionOperator personIdFilter = new SelectionOperator(
                    peopleScan,
                    new Predicate() {
                        @Override
                        public boolean test(Tuple tuple) {
                            String personId = tuple.getValue("People.personId");
                            return matchingPersonIds.contains(personId);
                        }
                    });

            // Build a map of person IDs to names
            Map<String, String> personIdToName = new HashMap<>();

            personIdFilter.open();
            while ((tuple = personIdFilter.next()) != null) {
                personIdToName.put(tuple.getValue("People.personId"), tuple.getValue("People.name"));
            }
            personIdFilter.close();

            // STEP 4: Join and output results
            // In-memory join using the collected data
            for (Tuple movieTuple : matchingMovies) {
                String movieId = movieTuple.getValue("Movies.movieId");
                String title = movieTuple.getValue("Movies.title");

                for (Tuple workedOnTuple : matchingWorkedOn) {
                    if (movieId.equals(workedOnTuple.getValue("WorkedOn.movieId"))) {
                        String personId = workedOnTuple.getValue("WorkedOn.personId");
                        String name = personIdToName.get(personId);

                        if (name != null) {
                            results.add(title + "," + name);
                        }
                    }
                }
            }

            long endTime = System.currentTimeMillis();
            long executionTime = endTime - startTime;

            // Find the corresponding test case and update its execution time
            for (TestCase testCase : TestCase.allTestCases) {
                if (testCase.startRange.equals(startRange) && testCase.endRange.equals(endRange)) {
                    testCase.executionTime = executionTime;
                    break;
                }
            }

        } catch (Exception e) {
            System.err.println("Error executing optimized query: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }

    /**
     * Calculates the selectivity of a range predicate on the Movies table.
     * 
     * @param bufferManager The buffer manager
     * @param startRange    The lower bound for the title range
     * @param endRange      The upper bound for the title range
     * @return The selectivity (fraction of tuples that satisfy the predicate)
     */
    private static double calculateSelectivity(
            ExtendedBufferManager bufferManager, String startRange, String endRange) {
        System.out.println("Calculating selectivity for title range [" + startRange + ", " + endRange + "]...");

        // Create operators
        ScanOperator moviesScan = new ScanOperator(
                bufferManager,
                MOVIES_FILE,
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

    /**
     * Class representing a test case.
     */
    private static class TestCase {
        static List<TestCase> allTestCases = new ArrayList<>();

        final String id;
        final String startRange;
        final String endRange;
        final String description;
        long executionTime;
        long measuredIO;
        long estimatedIO;
        double selectivity;
        int resultCount;

        public TestCase(String startRange, String endRange, String description) {
            this.id = startRange + "-" + endRange;
            this.startRange = startRange;
            this.endRange = endRange;
            this.description = description;
            this.executionTime = 0;
            this.measuredIO = 0;
            this.estimatedIO = 0;
            this.selectivity = 0.0;
            this.resultCount = 0;

            allTestCases.add(this);
        }
    }
}