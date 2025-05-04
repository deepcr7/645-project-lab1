package buffermanager;

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
    private static final int DEFAULT_BUFFER_SIZE = 5000;

    /**
     * Main method for the pre-process command.
     * 
     * @param args Command line arguments (not used)
     */
    public static void main(String[] args) {
        System.out.println("Initializing database files...");
        // Delete any existing files first
        java.io.File workedOnFile = new java.io.File(WORKEDON_FILE);
        if (workedOnFile.exists()) {
            workedOnFile.delete();
        }
        java.io.File peopleFile = new java.io.File(PEOPLE_FILE);
        if (peopleFile.exists()) {
            peopleFile.delete();
        }
        System.out.println("Starting pre-processing...");

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

        // Check IMDB dataset files
        System.out.println("Checking IMDB dataset files...");
        java.io.File titleBasicsFile = new java.io.File(TITLE_BASICS_FILE);
        java.io.File titlePrincipalsFile = new java.io.File(TITLE_PRINCIPALS_FILE);
        java.io.File nameBasicsFile = new java.io.File(NAME_BASICS_FILE);

        if (!titleBasicsFile.exists()) {
            System.err.println("Error: " + TITLE_BASICS_FILE + " not found.");
            return;
        }
        if (!titlePrincipalsFile.exists()) {
            System.err.println("Error: " + TITLE_PRINCIPALS_FILE + " not found.");
            return;
        }
        if (!nameBasicsFile.exists()) {
            System.err.println("Error: " + NAME_BASICS_FILE + " not found.");
            return;
        }

        // Load Movies table
        System.out.println("Loading Movies table...");
        long startTime = System.currentTimeMillis();
        int moviesLoaded = Utilities.loadDataset(bufferManager, MOVIES_FILE, TITLE_BASICS_FILE);
        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Loaded " + moviesLoaded + " movies in " + elapsedTime + " ms");

        // Load WorkedOn table
        System.out.println("Loading WorkedOn table...");
        startTime = System.currentTimeMillis();
        int workedOnLoaded = IMDBDataLoader.loadWorkedOnData(bufferManager, TITLE_PRINCIPALS_FILE);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Loaded " + workedOnLoaded + " WorkedOn records in " + elapsedTime + " ms");

        // Load People table
        System.out.println("Loading People table...");
        startTime = System.currentTimeMillis();
        int peopleLoaded = IMDBDataLoader.loadPeopleData(bufferManager, NAME_BASICS_FILE);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Loaded " + peopleLoaded + " people in " + elapsedTime + " ms");

        // Force all pages to disk
        System.out.println("Forcing all pages to disk...");
        bufferManager.force(MOVIES_FILE);
        bufferManager.force(WORKEDON_FILE);
        bufferManager.force(PEOPLE_FILE);

        System.out.println("Pre-processing complete.");
        System.out.println("Force-flushing all data to disk...");
        bufferManager.force(MOVIES_FILE);
        bufferManager.force(WORKEDON_FILE);
        bufferManager.force(PEOPLE_FILE);
    }
}