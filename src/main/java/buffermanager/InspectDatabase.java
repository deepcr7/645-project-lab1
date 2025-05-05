package buffermanager;

/**
 * Utility class to inspect the database content.
 */
public class InspectDatabase {
    private static final String MOVIES_FILE = "imdb_movies.bin";
    private static final String WORKEDON_FILE = "imdb_workedon.bin";
    private static final String PEOPLE_FILE = "imdb_people.bin";

    public static void main(String[] args) {
        int bufferSize = 5000;
        ExtendedBufferManager bufferManager = new ExtendedBufferManagerImpl(bufferSize);

        // Check if A-B titles exist
        checkMoviesRange(bufferManager, "A", "B");

        // Check if directors exist
        checkDirectors(bufferManager);

        // Try executing a join directly to test
        testJoin(bufferManager);
    }

    private static void checkMoviesRange(ExtendedBufferManager bufferManager, String startRange, String endRange) {
        System.out.println("Checking for movies with titles from '" + startRange + "' to '" + endRange + "':");

        ScanOperator moviesScan = new ScanOperator(
                bufferManager,
                MOVIES_FILE,
                PageFactory.TableType.MOVIES,
                new String[] { "Movies.movieId", "Movies.title" });

        SelectionOperator selection = new SelectionOperator(
                moviesScan,
                new RangePredicate("Movies.title", startRange, endRange));

        selection.open();
        Tuple tuple;
        int count = 0;

        while ((tuple = selection.next()) != null && count < 10) {
            System.out.println("  - " + tuple.getValue("Movies.title") +
                    " (ID: " + tuple.getValue("Movies.movieId") + ")");
            count++;
        }

        // Count the total
        int total = count;
        while (selection.next() != null) {
            total++;
        }

        selection.close();

        System.out.println("Total movies in range: " + total +
                (count < total ? " (only showing first " + count + ")" : ""));
    }

    private static void checkDirectors(ExtendedBufferManager bufferManager) {
        System.out.println("\nChecking for director entries in WorkedOn table:");

        ScanOperator workedOnScan = new ScanOperator(
                bufferManager,
                WORKEDON_FILE,
                PageFactory.TableType.WORKEDON,
                new String[] { "WorkedOn.movieId", "WorkedOn.personId", "WorkedOn.category" });

        SelectionOperator selection = new SelectionOperator(
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

        selection.open();
        Tuple tuple;
        int count = 0;

        while ((tuple = selection.next()) != null && count < 10) {
            System.out.println("  - MovieID: " + tuple.getValue("WorkedOn.movieId") +
                    ", PersonID: " + tuple.getValue("WorkedOn.personId") +
                    ", Category: " + tuple.getValue("WorkedOn.category"));
            count++;
        }

        // Count the total
        int total = count;
        while (selection.next() != null) {
            total++;
        }

        selection.close();

        System.out.println("Total director entries: " + total +
                (count < total ? " (only showing first " + count + ")" : ""));
    }

    private static void testJoin(ExtendedBufferManager bufferManager) {
        System.out.println("\nTesting manual join between Movies and WorkedOn tables:");

        try {
            // First, find a movie in range A-B
            ScanOperator moviesScan = new ScanOperator(
                    bufferManager,
                    MOVIES_FILE,
                    PageFactory.TableType.MOVIES,
                    new String[] { "Movies.movieId", "Movies.title" });

            SelectionOperator moviesSelection = new SelectionOperator(
                    moviesScan,
                    new RangePredicate("Movies.title", "A", "B"));

            moviesSelection.open();
            Tuple movie = moviesSelection.next();

            if (movie == null) {
                System.out.println("No movies found in range A-B to test join");
                moviesSelection.close();
                return;
            }

            String movieId = movie.getValue("Movies.movieId");
            String title = movie.getValue("Movies.title");
            System.out.println("Test movie: " + title + " (ID: " + movieId + ")");

            moviesSelection.close();

            // Now find directors for this movie
            ScanOperator workedOnScan = new ScanOperator(
                    bufferManager,
                    WORKEDON_FILE,
                    PageFactory.TableType.WORKEDON,
                    new String[] { "WorkedOn.movieId", "WorkedOn.personId", "WorkedOn.category" });

            workedOnScan.open();
            Tuple workedOn;
            boolean foundDirector = false;

            while ((workedOn = workedOnScan.next()) != null && !foundDirector) {
                String workedOnMovieId = workedOn.getValue("WorkedOn.movieId");
                String category = workedOn.getValue("WorkedOn.category");

                if (movieId.equals(workedOnMovieId) &&
                        category != null && category.toLowerCase().contains("direct")) {

                    String personId = workedOn.getValue("WorkedOn.personId");
                    System.out.println("Found director entry: MovieID=" + workedOnMovieId +
                            ", PersonID=" + personId + ", Category=" + category);

                    // Look up person
                    ScanOperator peopleScan = new ScanOperator(
                            bufferManager,
                            PEOPLE_FILE,
                            PageFactory.TableType.PEOPLE,
                            new String[] { "People.personId", "People.name" });

                    peopleScan.open();
                    Tuple person;

                    while ((person = peopleScan.next()) != null) {
                        String peoplePersonId = person.getValue("People.personId");
                        if (personId.equals(peoplePersonId)) {
                            String name = person.getValue("People.name");
                            System.out.println("Complete join result: " +
                                    title + " directed by " + name);
                            break;
                        }
                    }

                    peopleScan.close();
                    foundDirector = true;
                }
            }

            workedOnScan.close();

            if (!foundDirector) {
                System.out.println("No director found for movie: " + title);
            }

        } catch (Exception e) {
            System.err.println("Error testing join: " + e.getMessage());
            e.printStackTrace();
        }
    }
}