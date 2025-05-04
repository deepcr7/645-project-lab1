package buffermanager;

/**
 * Command line utility for running a query against the database.
 * The query to execute is:
 * SELECT title, name
 * FROM Movies, WorkedOn, People
 * WHERE title >= start-range AND title <= end-range AND category = "director"
 * AND Movies.movieId = WorkedOn.movieId AND WorkedOn.personId = People.personID
 */
public class RunQuery {
    // Default buffer size
    private static final int DEFAULT_BUFFER_SIZE = 1000;

    /**
     * Main method for the run_query command.
     * 
     * @param args Command line arguments:
     *             args[0] - start range for title
     *             args[1] - end range for title
     *             args[2] - buffer size (optional)
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: run_query start_range end_range [buffer_size]");
            System.exit(1);
        }

        String startRange = args[0];
        String endRange = args[1];

        int bufferSize = DEFAULT_BUFFER_SIZE;
        if (args.length > 2) {
            try {
                bufferSize = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid buffer size: " + args[2] + ". Using default: " + DEFAULT_BUFFER_SIZE);
                bufferSize = DEFAULT_BUFFER_SIZE;
            }
        }

        // Create buffer manager
        ExtendedBufferManager bufferManager = new ExtendedBufferManagerImpl(bufferSize);

        // Create and execute query
        QueryExecutor queryExecutor = new QueryExecutor(bufferManager, startRange, endRange, bufferSize);

        try {
            queryExecutor.execute();
        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}