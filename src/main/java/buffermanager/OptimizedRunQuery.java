package buffermanager;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OptimizedRunQuery {
    private static final String MOVIES_FILE = "imdb_movies.bin";
    private static final String WORKEDON_FILE = "imdb_workedon.bin";
    private static final String PEOPLE_FILE = "imdb_people.bin";
    private static final String TEMP_DIR = "temp";
    private static final String TEMP_FILTERED_WORKEDON_FILE = TEMP_DIR + "/filtered_workedon.bin";

    private static final int DEFAULT_BUFFER_SIZE = 1000;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: OptimizedRunQuery start_range end_range [buffer_size]");
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

        // Create temp directory if it doesn't exist
        new File(TEMP_DIR).mkdirs();

        // Create buffer manager
        ExtendedBufferManager bufferManager = new ExtendedBufferManagerImpl(bufferSize);

        try {
            long totalStartTime = System.currentTimeMillis();

            // STEP 1: Find all movies in the range and collect their IDs
            long stepStartTime = System.currentTimeMillis();

            ScanOperator moviesScan = new ScanOperator(
                    bufferManager,
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

            long stepTime = System.currentTimeMillis() - stepStartTime;

            // STEP 2: Filter WorkedOn to only directors from matching movies
            stepStartTime = System.currentTimeMillis();

            // Filter WorkedOn by category = 'director' and matching movie IDs
            ScanOperator workedOnScan = new ScanOperator(
                    bufferManager,
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

            stepTime = System.currentTimeMillis() - stepStartTime;

            // STEP 3: Find people names for matching person IDs
            stepStartTime = System.currentTimeMillis();

            ScanOperator peopleScan = new ScanOperator(
                    bufferManager,
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
            java.util.Map<String, String> personIdToName = new java.util.HashMap<>();

            personIdFilter.open();
            while ((tuple = personIdFilter.next()) != null) {
                personIdToName.put(tuple.getValue("People.personId"), tuple.getValue("People.name"));
            }
            personIdFilter.close();

            stepTime = System.currentTimeMillis() - stepStartTime;

            // STEP 4: Join and output results
            stepStartTime = System.currentTimeMillis();

            // In-memory join using the collected data
            System.out.println("\ntitle,name");
            int resultCount = 0;

            for (Tuple movieTuple : matchingMovies) {
                String movieId = movieTuple.getValue("Movies.movieId");
                String title = movieTuple.getValue("Movies.title");

                for (Tuple workedOnTuple : matchingWorkedOn) {
                    if (movieId.equals(workedOnTuple.getValue("WorkedOn.movieId"))) {
                        String personId = workedOnTuple.getValue("WorkedOn.personId");
                        String name = personIdToName.get(personId);

                        if (name != null) {
                            System.out.println(title + "," + name);
                            resultCount++;
                        }
                    }
                }
            }

            stepTime = System.currentTimeMillis() - stepStartTime;
            long totalTime = System.currentTimeMillis() - totalStartTime;
        } catch (Exception e) {
            System.err.println("Error executing optimized query: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}