package buffermanager;

import java.io.File;
import java.util.Iterator;

public class Assignment2Main {
    private static final String DATA_FILE = "imdb_database.bin";
    private static final String TITLE_INDEX_FILE = "imdb_title_index.bin";
    private static final String MOVIEID_INDEX_FILE = "imdb_movieid_index.bin";
    private static final int BUFFER_SIZE = 50000;
    private static final boolean USE_SAMPLE_DATA = true;
    private static final int SAMPLE_SIZE = 10000;
    private static boolean indexesCreated = false;

    private static ExtendedBufferManager bufferManager;
    private static Catalog catalog;

    public static void main(String[] args) {

        // Initialize buffer manager and catalog
        setup();

        // Run correctness tests
        runCorrectnessTests();

        // Run performance tests
        runPerformanceTests();

    }

    private static void setup() {
        bufferManager = new ExtendedBufferManagerImpl(BUFFER_SIZE);

        catalog = new Catalog();

        catalog.addTable("Movies", DATA_FILE, new String[] { "movieId", "title" });

        File dataFile = new File(DATA_FILE);
        if (!dataFile.exists()) {

            int rowsLoaded = Utilities.loadDataset(bufferManager, "title.basics.tsv");
        } else {
        }
    }

    private static void runCorrectnessTests() {
        if (!indexesCreated) {
            runTestC1();
            runTestC2();
            indexesCreated = true;
        }
        runTestC3();
        runTestC4();
    }

    private static void runTestC1() {

        BTree<String, Rid> titleIndex = createTitleIndex();

        catalog.addIndex("MoviesTitleIndex", "Movies", "title", TITLE_INDEX_FILE);

    }

    private static void runTestC2() {

        BTree<String, Rid> movieIdIndex = createMovieIdIndex();

        catalog.addIndex("MoviesMovieIdIndex", "Movies", "movieId", MOVIEID_INDEX_FILE);

    }

    private static void runTestC3() {

        BTree<String, Rid> titleIndex = new BTreeImpl<>(bufferManager, TITLE_INDEX_FILE);
        BTree<String, Rid> movieIdIndex = new BTreeImpl<>(bufferManager, MOVIEID_INDEX_FILE);

        String targetTitle = "The Godfather";

        Iterator<Rid> titleResults = titleIndex.search(targetTitle);
        if (titleResults.hasNext()) {
            Rid rid = titleResults.next();
            Page page = bufferManager.getPage(DATA_FILE, rid.getPageId());
            Row row = page.getRow(rid.getSlotId());

            bufferManager.unpinPage(DATA_FILE, rid.getPageId());
        } else {
        }

        String targetMovieId = "tt0068646";

        Iterator<Rid> movieIdResults = movieIdIndex.search(targetMovieId);
        if (movieIdResults.hasNext()) {
            Rid rid = movieIdResults.next();
            Page page = bufferManager.getPage(DATA_FILE, rid.getPageId());
            Row row = page.getRow(rid.getSlotId());

            bufferManager.unpinPage(DATA_FILE, rid.getPageId());
        } else {
        }

    }

    private static void runTestC4() {

        BTree<String, Rid> titleIndex = new BTreeImpl<>(bufferManager, TITLE_INDEX_FILE);
        BTree<String, Rid> movieIdIndex = new BTreeImpl<>(bufferManager, MOVIEID_INDEX_FILE);

        String startTitle = "Star Wars";
        String endTitle = "Star Wars: The Last Jedi";

        Iterator<Rid> titleResults = titleIndex.rangeSearch(startTitle, endTitle);
        int titleCount = 0;

        while (titleResults.hasNext() && titleCount < 5) {
            Rid rid = titleResults.next();
            Page page = bufferManager.getPage(DATA_FILE, rid.getPageId());
            Row row = page.getRow(rid.getSlotId());

            bufferManager.unpinPage(DATA_FILE, rid.getPageId());
            titleCount++;
        }

        String startMovieId = "tt0076759";
        String endMovieId = "tt0080684";

        Iterator<Rid> movieIdResults = movieIdIndex.rangeSearch(startMovieId, endMovieId);
        int movieIdCount = 0;

        while (movieIdResults.hasNext() && movieIdCount < 5) {
            Rid rid = movieIdResults.next();
            Page page = bufferManager.getPage(DATA_FILE, rid.getPageId());
            Row row = page.getRow(rid.getSlotId());

            bufferManager.unpinPage(DATA_FILE, rid.getPageId());
            movieIdCount++;
        }

        if (movieIdResults.hasNext()) {
        }

    }

    private static BTree<String, Rid> createTitleIndex() {
        BTree<String, Rid> titleIndex = new BTreeImpl<>(bufferManager, TITLE_INDEX_FILE);

        File dataFile = new File(DATA_FILE);
        int totalPages = (int) (dataFile.length() / PageImpl.PAGE_SIZE);

        int rowCount = 0;
        int maxRows = USE_SAMPLE_DATA ? SAMPLE_SIZE : Integer.MAX_VALUE;

        outer: for (int pageId = 0; pageId < totalPages; pageId++) {
            Page page = bufferManager.getPage(DATA_FILE, pageId);
            if (page != null) {
                int rows = getRowCount(page);
                for (int rowId = 0; rowId < rows; rowId++) {
                    Row row = page.getRow(rowId);
                    if (row != null) {
                        String title = new String(row.title).trim();
                        Rid rid = new Rid(pageId, rowId);
                        titleIndex.insert(title, rid);
                        rowCount++;

                        // Check if we've reached the sample limit
                        if (rowCount >= maxRows) {
                            break outer;
                        }
                    }
                }
                bufferManager.unpinPage(DATA_FILE, pageId);
            }
        }

        bufferManager.force(TITLE_INDEX_FILE);
        return titleIndex;
    }

    private static BTree<String, Rid> createMovieIdIndex() {
        BTreeImpl<String> movieIdIndex = new BTreeImpl<>(bufferManager, MOVIEID_INDEX_FILE);

        movieIdIndex.startBulkLoading();

        File dataFile = new File(DATA_FILE);
        int totalPages = (int) (dataFile.length() / PageImpl.PAGE_SIZE);

        int rowCount = 0;
        int maxRows = USE_SAMPLE_DATA ? SAMPLE_SIZE : Integer.MAX_VALUE;

        outer: for (int pageId = 0; pageId < totalPages; pageId++) {
            Page page = bufferManager.getPage(DATA_FILE, pageId);
            if (page != null) {
                int rows = getRowCount(page);
                for (int rowId = 0; rowId < rows; rowId++) {
                    Row row = page.getRow(rowId);
                    if (row != null) {
                        String movieId = new String(row.movieId).trim();
                        Rid rid = new Rid(pageId, rowId);
                        movieIdIndex.insert(movieId, rid);
                        rowCount++;

                        if (rowCount >= maxRows) {
                            break outer;
                        }
                    }
                }
                bufferManager.unpinPage(DATA_FILE, pageId);
            }
        }

        movieIdIndex.endBulkLoading();

        bufferManager.force(MOVIEID_INDEX_FILE);
        return movieIdIndex;
    }

    private static int getRowCount(Page page) {
        int count = 0;
        while (true) {
            try {
                Row row = page.getRow(count);
                if (row == null) {
                    break;
                }
                count++;
            } catch (Exception e) {
                break;
            }
        }
        return count;
    }

    private static void runPerformanceTests() {
        PerformanceTest.main(null);
    }
}