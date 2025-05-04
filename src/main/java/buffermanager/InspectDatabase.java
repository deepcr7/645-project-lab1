package buffermanager;

/**
 * Utility to inspect the database files.
 */
public class InspectDatabase {
    private static final String MOVIES_FILE = "imdb_movies.bin";
    private static final String WORKEDON_FILE = "imdb_workedon.bin";
    private static final String PEOPLE_FILE = "imdb_people.bin";

    public static void main(String[] args) {
        int bufferSize = 5000;
        ExtendedBufferManager bufferManager = new ExtendedBufferManagerImpl(bufferSize);

        // Check Movies table
        System.out.println("Checking Movies table...");
        checkMovies(bufferManager);

        // Check WorkedOn table
        System.out.println("\nChecking WorkedOn table...");
        checkWorkedOn(bufferManager);

        // Check People table
        System.out.println("\nChecking People table...");
        checkPeople(bufferManager);
    }

    private static void checkMovies(ExtendedBufferManager bufferManager) {
        ScanOperator scan = new ScanOperator(
                bufferManager,
                MOVIES_FILE,
                PageFactory.TableType.MOVIES,
                new String[] { "Movies.movieId", "Movies.title" });

        scan.open();
        int count = 0;
        Tuple tuple;
        while ((tuple = scan.next()) != null && count < 10) {
            System.out.println("Movie: " + tuple);
            count++;
        }

        // Count total
        int total = count;
        while (scan.next() != null) {
            total++;
        }
        scan.close();

        System.out.println("Total movies: " + total);
    }

    private static void checkWorkedOn(ExtendedBufferManager bufferManager) {
        ScanOperator scan = new ScanOperator(
                bufferManager,
                WORKEDON_FILE,
                PageFactory.TableType.WORKEDON,
                new String[] { "WorkedOn.movieId", "WorkedOn.personId", "WorkedOn.category" });

        scan.open();
        int count = 0;
        int directorCount = 0;
        Tuple tuple;
        while ((tuple = scan.next()) != null && count < 50) {
            String category = tuple.getValue("WorkedOn.category");
            if (category != null && category.toLowerCase().contains("direct")) {
                System.out.println("Director: " + tuple);
                directorCount++;
            }
            count++;
        }

        // Count totals
        int total = count;
        int totalDirectors = directorCount;
        while ((tuple = scan.next()) != null) {
            total++;
            String category = tuple.getValue("WorkedOn.category");
            if (category != null && category.toLowerCase().contains("direct")) {
                totalDirectors++;
            }
        }
        scan.close();

        System.out.println("Total WorkedOn records: " + total);
        System.out.println("Total directors: " + totalDirectors);
    }

    private static void checkPeople(ExtendedBufferManager bufferManager) {
        ScanOperator scan = new ScanOperator(
                bufferManager,
                PEOPLE_FILE,
                PageFactory.TableType.PEOPLE,
                new String[] { "People.personId", "People.name" });

        scan.open();
        int count = 0;
        Tuple tuple;
        while ((tuple = scan.next()) != null && count < 10) {
            System.out.println("Person: " + tuple);
            count++;
        }

        // Count total
        int total = count;
        while (scan.next() != null) {
            total++;
        }
        scan.close();

        System.out.println("Total people: " + total);
    }
}