package buffermanager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class BTreeTest {
    private static final String DATA_FILE = "test_movies.bin";
    private static final String TITLE_INDEX_FILE = "test_movies_title_index.bin";
    private static final String MOVIEID_INDEX_FILE = "test_movies_movieid_index.bin";
    private static final int BUFFER_SIZE = 500;

    private ExtendedBufferManager bufferManager;
    private BTree<String, Rid> titleIndex;
    private BTree<String, Rid> movieIdIndex;

    @BeforeEach
    public void setUp() {
        bufferManager = new ExtendedBufferManagerImpl(BUFFER_SIZE);
        createTestData();
        titleIndex = new BTreeImpl<>(bufferManager, TITLE_INDEX_FILE);
        movieIdIndex = new BTreeImpl<>(bufferManager, MOVIEID_INDEX_FILE);
        buildIndexes();
    }

    @AfterEach
    public void tearDown() {
        File dataFile = new File(DATA_FILE);
        if (dataFile.exists()) {
            dataFile.delete();
        }

        File titleIndexFile = new File(TITLE_INDEX_FILE);
        if (titleIndexFile.exists()) {
            titleIndexFile.delete();
        }

        File movieIdIndexFile = new File(MOVIEID_INDEX_FILE);
        if (movieIdIndexFile.exists()) {
            movieIdIndexFile.delete();
        }
    }

    /**
     * Creates 2 pages and inserts 3 movie records into each.
     * Each record includes a movie ID and a title.
     */
    private void createTestData() {
        for (int i = 0; i < 2; i++) {
            Page page = bufferManager.createPage(DATA_FILE);
            assertNotNull(page, "Failed to create page");
            for (int j = 0; j < 3; j++) {
                String movieId = String.format("tt%07d", i * 3 + j);
                String title = "Movie " + (i * 3 + j);

                Row row = new Row(movieId, title);
                int rowId = page.insertRow(row);

                assertEquals(j, rowId, "Row ID should match insertion order");
                Row verifyRow = page.getRow(rowId);
                assertNotNull(verifyRow, "Row should be retrievable after insertion");
            }

            bufferManager.markDirty(DATA_FILE, page.getPid());
            bufferManager.unpinPage(DATA_FILE, page.getPid());
        }
    }

    /**
     * Builds two BTree indexes:
     * - titleIndex maps titles to their corresponding RIDs.
     * - movieIdIndex maps movie IDs to their corresponding RIDs.
     */
    private void buildIndexes() {
        for (int pageId = 0; pageId < 2; pageId++) {
            Page page = bufferManager.getPage(DATA_FILE, pageId);
            assertNotNull(page, "Failed to load page from data file");

            for (int rowId = 0; rowId < 3; rowId++) {
                Row row = page.getRow(rowId);
                if (row == null) {
                    System.err.println("Warning: Null row at page " + pageId + ", row " + rowId);
                    continue;
                }

                String title = new String(row.title).trim();
                Rid rid = new Rid(pageId, rowId);

                titleIndex.insert(title, rid);
            }

            bufferManager.unpinPage(DATA_FILE, pageId);
        }
        for (int pageId = 0; pageId < 2; pageId++) {
            Page page = bufferManager.getPage(DATA_FILE, pageId);
            assertNotNull(page, "Failed to load page from data file");

            for (int rowId = 0; rowId < 3; rowId++) {
                Row row = page.getRow(rowId);
                if (row == null) {
                    System.err.println("Warning: Null row at page " + pageId + ", row " + rowId);
                    continue;
                }

                String movieId = new String(row.movieId).trim();
                Rid rid = new Rid(pageId, rowId);

                movieIdIndex.insert(movieId, rid);
            }

            bufferManager.unpinPage(DATA_FILE, pageId);
        }
    }

    /**
     * Test Case C1:
     * Verifies that the title index file is created after index construction.
     */
    @Test
    public void testC1_CreateTitleIndex() {
        assertTrue(new File(TITLE_INDEX_FILE).exists(), "Title index file should exist");
    }

    /**
     * Test Case C2:
     * Verifies that the movie ID index file is created after index construction.
     */
    @Test
    public void testC2_CreateMovieIdIndex() {
        assertTrue(new File(MOVIEID_INDEX_FILE).exists(), "MovieId index file should exist");
    }

    /**
     * Test Case C3:
     * Performs an exact-match search on the title index for "Movie 3"
     * and verifies that the retrieved record matches the query.
     */
    @Test
    public void testC3_SearchSingleKey() {
        String targetTitle = "Movie 3";
        Iterator<Rid> titleResults = titleIndex.search(targetTitle);

        assertTrue(titleResults.hasNext(), "Should find at least one result");

        Rid rid = titleResults.next();
        Page page = bufferManager.getPage(DATA_FILE, rid.getPageId());
        Row row = page.getRow(rid.getSlotId());

        String foundTitle = new String(row.title).trim();
        assertEquals(targetTitle, foundTitle, "Retrieved title should match the search key");

        bufferManager.unpinPage(DATA_FILE, rid.getPageId());
    }

    /**
     * Test Case C4:
     * Performs a range search in the title index from "Movie 1" to "Movie 5"
     * and verifies that the number of results is correct and that all titles
     * fall within the specified range.
     */
    @Test
    public void testC4_SearchKeyRange() {
        String startTitle = "Movie 1";
        String endTitle = "Movie 5";

        Iterator<Rid> titleResults = titleIndex.rangeSearch(startTitle, endTitle);

        List<Rid> titleRids = new ArrayList<>();
        while (titleResults.hasNext()) {
            titleRids.add(titleResults.next());
        }

        assertEquals(5, titleRids.size(), "Should find 5 results in the range");
        for (Rid rid : titleRids) {
            Page page = bufferManager.getPage(DATA_FILE, rid.getPageId());
            Row row = page.getRow(rid.getSlotId());

            String foundTitle = new String(row.title).trim();
            assertTrue(foundTitle.compareTo(startTitle) >= 0 && foundTitle.compareTo(endTitle) <= 0,
                    "Retrieved title should be within the search range");

            bufferManager.unpinPage(DATA_FILE, rid.getPageId());
        }
    }
}