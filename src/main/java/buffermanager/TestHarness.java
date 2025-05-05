package buffermanager;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Comprehensive test harness for the query executor.
 * Runs correctness and performance tests.
 */
public class TestHarness {
    private static final String MOVIES_FILE = "imdb_movies.bin";
    private static final String WORKEDON_FILE = "imdb_workedon.bin";
    private static final String PEOPLE_FILE = "imdb_people.bin";
    private static final String TEST_RESULTS_DIR = "test_results";

    public static void main(String[] args) {
        System.out.println("======================================");
        System.out.println("Query Executor Test Harness");
        System.out.println("======================================");

        // Create test results directory
        createTestResultsDirectory();

        // Test different title ranges and buffer sizes
        TestCase[] testCases = {
                new TestCase("A", "C", 1000, "Small range (A-C), default buffer"),
                new TestCase("D", "F", 1000, "Small range (D-F), default buffer"),
                new TestCase("G", "K", 1000, "Medium range (G-K), default buffer"),
                new TestCase("L", "P", 1000, "Medium range (L-P), default buffer"),
                new TestCase("Q", "Z", 1000, "Large range (Q-Z), default buffer"),
                new TestCase("A", "Z", 1000, "Full range (A-Z), default buffer"),
                new TestCase("A", "Z", 5000, "Full range (A-Z), large buffer")
        };

        // Verify database files exist
        verifyDatabaseFiles();

        // Run each test case
        for (TestCase testCase : testCases) {
            runTestCase(testCase);
        }

        // Run performance tests
        System.out.println("\nRunning comprehensive performance tests...");
        PerformanceTestLab3.main(new String[] { "1000" });

        // Generate performance plots
        try {
            System.out.println("\nGenerating performance plots...");
            PlotGenerator.main(null);
        } catch (Exception e) {
            System.err.println("Error generating plots: " + e.getMessage());
            e.printStackTrace();
        }

        // Compare test results
        compareTestResults();

        System.out
                .println("\nTest harness complete. Check the " + TEST_RESULTS_DIR + " directory for detailed results.");
    }

    /**
     * Creates the test results directory if it doesn't exist.
     */
    private static void createTestResultsDirectory() {
        File testResultsDir = new File(TEST_RESULTS_DIR);
        if (!testResultsDir.exists()) {
            if (testResultsDir.mkdir()) {
                System.out.println("Created test results directory: " + testResultsDir.getAbsolutePath());
            } else {
                System.err.println("Failed to create test results directory!");
            }
        }
    }

    /**
     * Verifies that all database files exist.
     */
    private static void verifyDatabaseFiles() {
        System.out.println("\nVerifying database files...");

        File moviesFile = new File(MOVIES_FILE);
        File workedOnFile = new File(WORKEDON_FILE);
        File peopleFile = new File(PEOPLE_FILE);

        boolean allFilesExist = true;

        if (!moviesFile.exists()) {
            System.err.println("Error: " + MOVIES_FILE + " not found!");
            allFilesExist = false;
        } else {
            System.out.println("✓ Found " + MOVIES_FILE + " (" + formatFileSize(moviesFile.length()) + ")");
        }

        if (!workedOnFile.exists()) {
            System.err.println("Error: " + WORKEDON_FILE + " not found!");
            allFilesExist = false;
        } else {
            System.out.println("✓ Found " + WORKEDON_FILE + " (" + formatFileSize(workedOnFile.length()) + ")");
        }

        if (!peopleFile.exists()) {
            System.err.println("Error: " + PEOPLE_FILE + " not found!");
            allFilesExist = false;
        } else {
            System.out.println("✓ Found " + PEOPLE_FILE + " (" + formatFileSize(peopleFile.length()) + ")");
        }

        if (!allFilesExist) {
            System.err.println("\nOne or more database files are missing!");
            System.err.println("Please run PreProcess.java first to create the database files.");
            System.exit(1);
        }
    }

    /**
     * Runs a single test case.
     */
    private static void runTestCase(TestCase testCase) {
        System.out.println("\nTest case: " + testCase.description);
        System.out.println("Range: [" + testCase.startRange + ", " + testCase.endRange + "]");
        System.out.println("Buffer size: " + testCase.bufferSize);

        String outputFile = TEST_RESULTS_DIR + "/results_" + testCase.startRange + "_" +
                testCase.endRange + "_" + testCase.bufferSize + ".csv";

        try {
            // Set up to capture standard output
            PrintStream originalOut = System.out;
            PrintStream fileOut = new PrintStream(new FileOutputStream(outputFile));
            System.setOut(fileOut);

            // Create buffer manager and query executor
            ExtendedBufferManager bufferManager = new ExtendedBufferManagerImpl(testCase.bufferSize);
            IoCountingBufferManager ioCounter = new IoCountingBufferManager(bufferManager);
            QueryExecutor queryExecutor = new QueryExecutor(
                    ioCounter, testCase.startRange, testCase.endRange, testCase.bufferSize);

            // Execute query and measure time
            long startTime = System.currentTimeMillis();
            queryExecutor.execute();
            long endTime = System.currentTimeMillis();
            long executionTime = endTime - startTime;
            long ioCount = ioCounter.getIoCount();

            // Restore standard output
            System.setOut(originalOut);
            fileOut.close();

            // Calculate result statistics
            int resultCount = countLinesInFile(outputFile) - 1; // Subtract header line

            System.out.println("Query completed in " + executionTime + " ms");
            System.out.println("I/O operations: " + ioCount);
            System.out.println("Results: " + resultCount);
            System.out.println("Results written to: " + outputFile);

            // Add test case to summary
            testCase.executionTime = executionTime;
            testCase.ioCount = ioCount;
            testCase.resultCount = resultCount;

        } catch (Exception e) {
            System.err.println("Error running test case: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Compares all test results and generates a summary.
     */
    private static void compareTestResults() {
        System.out.println("\nComparing test results...");

        try {
            String summaryFile = TEST_RESULTS_DIR + "/summary.csv";
            PrintWriter writer = new PrintWriter(new FileWriter(summaryFile));

            // Write header
            writer.println(
                    "Start Range,End Range,Buffer Size,Results,I/O Operations,Execution Time (ms),I/O per Result");

            // Get all result files
            File testResultsDir = new File(TEST_RESULTS_DIR);
            File[] resultFiles = testResultsDir
                    .listFiles((dir, name) -> name.startsWith("results_") && name.endsWith(".csv"));

            if (resultFiles != null) {
                Arrays.sort(resultFiles);

                for (File file : resultFiles) {
                    String filename = file.getName();
                    String[] parts = filename.replace("results_", "").replace(".csv", "").split("_");

                    if (parts.length >= 3) {
                        String startRange = parts[0];
                        String endRange = parts[1];
                        int bufferSize = Integer.parseInt(parts[2]);

                        int resultCount = countLinesInFile(file.getAbsolutePath()) - 1; // Subtract header line

                        // Look for the corresponding test case
                        TestCase testCase = null;
                        for (TestCase tc : TestCase.allTestCases) {
                            if (tc.startRange.equals(startRange) && tc.endRange.equals(endRange) &&
                                    tc.bufferSize == bufferSize) {
                                testCase = tc;
                                break;
                            }
                        }

                        if (testCase != null) {
                            // Calculate I/O per result
                            double ioPerResult = testCase.resultCount > 0
                                    ? (double) testCase.ioCount / testCase.resultCount
                                    : 0;

                            writer.printf("%s,%s,%d,%d,%d,%d,%.2f%n",
                                    startRange, endRange, bufferSize,
                                    testCase.resultCount, testCase.ioCount, testCase.executionTime,
                                    ioPerResult);
                        }
                    }
                }
            }

            writer.close();
            System.out.println("Test summary written to: " + summaryFile);

        } catch (IOException e) {
            System.err.println("Error comparing test results: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Counts the number of lines in a file.
     */
    private static int countLinesInFile(String filename) {
        int lineCount = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            while (reader.readLine() != null) {
                lineCount++;
            }
        } catch (IOException e) {
            System.err.println("Error counting lines in file: " + e.getMessage());
        }
        return lineCount;
    }

    /**
     * Formats a file size in bytes to a human-readable string.
     */
    private static String formatFileSize(long size) {
        if (size < 1024) {
            return size + " bytes";
        } else if (size < 1024 * 1024) {
            return String.format("%.2f KB", size / 1024.0);
        } else if (size < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", size / (1024.0 * 1024));
        } else {
            return String.format("%.2f GB", size / (1024.0 * 1024 * 1024));
        }
    }

    /**
     * Class representing a test case.
     */
    private static class TestCase {
        static List<TestCase> allTestCases = new ArrayList<>();

        final String startRange;
        final String endRange;
        final int bufferSize;
        final String description;
        long executionTime;
        long ioCount;
        int resultCount;

        public TestCase(String startRange, String endRange, int bufferSize, String description) {
            this.startRange = startRange;
            this.endRange = endRange;
            this.bufferSize = bufferSize;
            this.description = description;
            this.executionTime = 0;
            this.ioCount = 0;
            this.resultCount = 0;

            allTestCases.add(this);
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