package buffermanager;

/**
 * A simple test program to verify the query execution.
 */
public class TestQuery {
    private static final String MOVIES_FILE = "imdb_movies.bin";
    private static final String WORKEDON_FILE = "imdb_workedon.bin";
    private static final String PEOPLE_FILE = "imdb_people.bin";

    public static void main(String[] args) {
        int bufferSize = 5000;
        ExtendedBufferManager bufferManager = new ExtendedBufferManagerImpl(bufferSize);

        // Test directors for movies with titles A-B
        System.out.println("Testing directors for movies with titles A-B...");

        // Create operators
        ScanOperator moviesScan = new ScanOperator(
                bufferManager,
                MOVIES_FILE,
                PageFactory.TableType.MOVIES,
                new String[] { "Movies.movieId", "Movies.title" });

        ScanOperator workedOnScan = new ScanOperator(
                bufferManager,
                WORKEDON_FILE,
                PageFactory.TableType.WORKEDON,
                new String[] { "WorkedOn.movieId", "WorkedOn.personId", "WorkedOn.category" });

        ScanOperator peopleScan = new ScanOperator(
                bufferManager,
                PEOPLE_FILE,
                PageFactory.TableType.PEOPLE,
                new String[] { "People.personId", "People.name" });

        // Find one director
        workedOnScan.open();
        Tuple tuple;
        boolean foundDirector = false;
        String directorMovieId = "";
        String directorPersonId = "";

        while ((tuple = workedOnScan.next()) != null && !foundDirector) {
            String category = tuple.getValue("WorkedOn.category");
            if (category != null && category.toLowerCase().contains("direct")) {
                directorMovieId = tuple.getValue("WorkedOn.movieId");
                directorPersonId = tuple.getValue("WorkedOn.personId");
                foundDirector = true;
            }
        }
        workedOnScan.close();

        if (foundDirector) {
            // Find the movie
            moviesScan.open();
            boolean foundMovie = false;
            String movieTitle = "";

            while ((tuple = moviesScan.next()) != null && !foundMovie) {
                String movieId = tuple.getValue("Movies.movieId");
                if (movieId != null && movieId.equals(directorMovieId)) {
                    movieTitle = tuple.getValue("Movies.title");
                    foundMovie = true;
                }
            }
            moviesScan.close();

            // Find the person
            peopleScan.open();
            boolean foundPerson = false;
            String personName = "";

            while ((tuple = peopleScan.next()) != null && !foundPerson) {
                String personId = tuple.getValue("People.personId");
                if (personId != null && personId.equals(directorPersonId)) {
                    personName = tuple.getValue("People.name");
                    foundPerson = true;
                }
            }
            peopleScan.close();

            if (foundMovie && foundPerson) {
                System.out.println("\nVerified join data: ");
                System.out.println("Movie: " + movieTitle);
                System.out.println("Director: " + personName);
            }
        }
    }
}