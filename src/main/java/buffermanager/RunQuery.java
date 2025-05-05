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
        System.out.println("RunQuery main() started");
        if (args.length < 2) {
            System.err.println("Usage: run_query start_range end_range [buffer_size]");
            System.err.println("Example: run_query A Z 1000");
            System.exit(1);
        }

        String startRange = args[0];
        String endRange = args[1];

        int bufferSize = DEFAULT_BUFFER_SIZE;
        if (args.length > 2) {
            try {
                bufferSize = Integer.parseInt(args[2]);
                if (bufferSize <= 0) {
                    System.err.println("Buffer size must be positive. Using default: " + DEFAULT_BUFFER_SIZE);
                    bufferSize = DEFAULT_BUFFER_SIZE;
                }
            } catch (NumberFormatException e) {
                System.err.println("Invalid buffer size: " + args[2] + ". Using default: " + DEFAULT_BUFFER_SIZE);
                bufferSize = DEFAULT_BUFFER_SIZE;
            }
        }

        // Create buffer manager
        ExtendedBufferManager bufferManager = new ExtendedBufferManagerImpl(bufferSize);

        // Print query details
        System.out.println("Executing query:");
        System.out.println("SELECT title, name");
        System.out.println("FROM Movies, WorkedOn, People");
        System.out.println("WHERE title >= '" + startRange + "' AND title <= '" + endRange + "'");
        System.out.println("  AND category = 'director'");
        System.out.println("  AND Movies.movieId = WorkedOn.movieId");
        System.out.println("  AND WorkedOn.personId = People.personId");
        System.out.println();
        System.out.println("Buffer size: " + bufferSize);
        System.out.println();

        // Create and execute query
        QueryExecutor queryExecutor = new QueryExecutor(bufferManager, startRange, endRange, bufferSize);

        try {
            long startTime = System.currentTimeMillis();
            queryExecutor.execute();
            long endTime = System.currentTimeMillis();
            long executionTime = endTime - startTime;

            System.out.println("\nQuery execution completed in " + executionTime + " ms");

        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}