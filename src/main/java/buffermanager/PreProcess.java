package buffermanager;

import java.io.File;

/**
 * Command line utility for pre-processing the database.
 * This loads the IMDB dataset files into the database tables and creates the
 * necessary indexes.
 */
public class PreProcess {
    // File paths for the IMDB dataset files
    private static final String TITLE_BASICS_FILE = "title.basics.tsv";
    private static final String TITLE_PRINCIPALS_FILE = "title.principals.tsv";
    private static final String NAME_BASICS_FILE = "name.basics.tsv";

    // File paths for the database files
    private static final String MOVIES_FILE = "imdb_movies.bin";
    private static final String WORKEDON_FILE = "imdb_workedon.bin";
    private static final String PEOPLE_FILE = "imdb_people.bin";

    // Default buffer size
    private static final int DEFAULT_BUFFER_SIZE = 10000;

    /**
     * Main method for the pre-process command.
     * 
     * @param args Command line arguments:
     *             args[0] - buffer size (optional)
     */
    public static void main(String[] args) {
        System.out.println("======================================");
        System.out.println("IMDB Database Pre-Processing Utility");
        System.out.println("======================================");

        int bufferSize = DEFAULT_BUFFER_SIZE;
        if (args.length > 0) {
            try {
                bufferSize = Integer.parseInt(args[0]);
                if (bufferSize <= 0) {
                    System.err.println("Buffer size must be positive. Using default: " + DEFAULT_BUFFER_SIZE);
                    bufferSize = DEFAULT_BUFFER_SIZE;
                }
            } catch (NumberFormatException e) {
                System.err.println("Invalid buffer size: " + args[0] + ". Using default: " + DEFAULT_BUFFER_SIZE);
                bufferSize = DEFAULT_BUFFER_SIZE;
            }
        }

        System.out.println("Using buffer size: " + bufferSize);

        // Check IMDB dataset files
        System.out.println("\nChecking IMDB dataset files...");
        checkDatasetFiles();

        // Create buffer manager
        ExtendedBufferManager bufferManager = new ExtendedBufferManagerImpl(bufferSize);

        // Process each table
        processMoviesTable(bufferManager);
        processWorkedOnTable(bufferManager);
        processPeopleTable(bufferManager);

        System.out.println("\nPre-processing complete!");
        System.out.println("The database is now ready for queries using:");
        System.out.println("  run_query start_range end_range [buffer_size]");
    }

    /**
     * Checks if all required dataset files exist.
     */
    private static void checkDatasetFiles() {
        File titleBasicsFile = new File(TITLE_BASICS_FILE);
        File titlePrincipalsFile = new File(TITLE_PRINCIPALS_FILE);
        File nameBasicsFile = new File(NAME_BASICS_FILE);

        boolean allFilesExist = true;

        if (!titleBasicsFile.exists()) {
            System.err.println("Error: " + TITLE_BASICS_FILE + " not found.");
            allFilesExist = false;
        } else {
            System.out.println("✓ Found " + TITLE_BASICS_FILE + " (" +
                    formatFileSize(titleBasicsFile.length()) + ")");
        }

        if (!titlePrincipalsFile.exists()) {
            System.err.println("Error: " + TITLE_PRINCIPALS_FILE + " not found.");
            allFilesExist = false;
        } else {
            System.out.println("✓ Found " + TITLE_PRINCIPALS_FILE + " (" +
                    formatFileSize(titlePrincipalsFile.length()) + ")");
        }

        if (!nameBasicsFile.exists()) {
            System.err.println("Error: " + NAME_BASICS_FILE + " not found.");
            allFilesExist = false;
        } else {
            System.out.println("✓ Found " + NAME_BASICS_FILE + " (" +
                    formatFileSize(nameBasicsFile.length()) + ")");
        }

        if (!allFilesExist) {
            System.err.println("\nOne or more dataset files are missing!");
            System.err.println("Please download the IMDB dataset files from https://www.imdb.com/interfaces/");
            System.err.println("and place them in the current directory.");
            System.exit(1);
        }
    }

    /**
     * Processes the Movies table.
     */
    private static void processMoviesTable(ExtendedBufferManager bufferManager) {
        System.out.println("\nProcessing Movies table...");

        // Delete existing file if it exists
        File moviesFile = new File(MOVIES_FILE);
        if (moviesFile.exists()) {
            if (moviesFile.delete()) {
                System.out.println("Deleted existing Movies table file.");
            } else {
                System.err.println("Warning: Could not delete existing Movies table file.");
            }
        }

        // Load the data
        long startTime = System.currentTimeMillis();
        int moviesLoaded = Utilities.loadDataset(bufferManager, MOVIES_FILE, TITLE_BASICS_FILE);
        long elapsedTime = System.currentTimeMillis() - startTime;

        System.out.println("Loaded " + moviesLoaded + " movies in " + elapsedTime + " ms");
        bufferManager.force(MOVIES_FILE);

        // Print file size
        moviesFile = new File(MOVIES_FILE);
        if (moviesFile.exists()) {
            System.out.println("Movies table size: " + formatFileSize(moviesFile.length()));
        }
    }

    /**
     * Processes the WorkedOn table.
     */
    private static void processWorkedOnTable(ExtendedBufferManager bufferManager) {
        System.out.println("\nProcessing WorkedOn table...");

        // Delete existing file if it exists
        File workedOnFile = new File(WORKEDON_FILE);
        if (workedOnFile.exists()) {
            if (workedOnFile.delete()) {
                System.out.println("Deleted existing WorkedOn table file.");
            } else {
                System.err.println("Warning: Could not delete existing WorkedOn table file.");
            }
        }

        // Load the data
        long startTime = System.currentTimeMillis();
        int workedOnLoaded = IMDBDataLoader.loadWorkedOnData(bufferManager, TITLE_PRINCIPALS_FILE);
        long elapsedTime = System.currentTimeMillis() - startTime;

        System.out.println("Loaded " + workedOnLoaded + " WorkedOn records in " + elapsedTime + " ms");
        bufferManager.force(WORKEDON_FILE);

        // Print file size
        workedOnFile = new File(WORKEDON_FILE);
        if (workedOnFile.exists()) {
            System.out.println("WorkedOn table size: " + formatFileSize(workedOnFile.length()));
        }
    }

    /**
     * Processes the People table.
     */
    private static void processPeopleTable(ExtendedBufferManager bufferManager) {
        System.out.println("\nProcessing People table...");

        // Delete existing file if it exists
        File peopleFile = new File(PEOPLE_FILE);
        if (peopleFile.exists()) {
            if (peopleFile.delete()) {
                System.out.println("Deleted existing People table file.");
            } else {
                System.err.println("Warning: Could not delete existing People table file.");
            }
        }

        // Load the data
        long startTime = System.currentTimeMillis();
        int peopleLoaded = IMDBDataLoader.loadPeopleData(bufferManager, NAME_BASICS_FILE);
        long elapsedTime = System.currentTimeMillis() - startTime;

        System.out.println("Loaded " + peopleLoaded + " people in " + elapsedTime + " ms");
        bufferManager.force(PEOPLE_FILE);

        // Print file size
        peopleFile = new File(PEOPLE_FILE);
        if (peopleFile.exists()) {
            System.out.println("People table size: " + formatFileSize(peopleFile.length()));
        }
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
}