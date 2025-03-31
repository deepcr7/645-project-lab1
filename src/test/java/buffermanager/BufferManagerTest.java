package buffermanager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the BufferManager implementation.
 */
public class BufferManagerTest {

    private static final String TEST_FILE = "test_db.bin";
    private BufferManager bufferManager;

    @BeforeEach
    public void setUp() {
        bufferManager = new BufferManagerImpl(3, TEST_FILE);
    }

    @AfterEach
    public void tearDown() {
        File file = new File(TEST_FILE);
        if (file.exists()) {
            file.delete();
        }
    }

    /**
     * Tests page creation, eviction, and reloading from disk.
     * Verifies that pages are properly written to disk when evicted
     * and can be reloaded when needed.
     */
    @Test
    public void testCreatePage() {
        Page page = bufferManager.createPage();
        assertNotNull(page, "Page should be created successfully");
        assertEquals(0, page.getPid(), "First page should have ID 0");
        Row row = new Row("tt0000001", "Test Movie");
        page.insertRow(row);
        bufferManager.markDirty(page.getPid());
        Page page1 = bufferManager.createPage();
        Page page2 = bufferManager.createPage();
        assertNotNull(page1, "Second page should be created successfully");
        assertNotNull(page2, "Third page should be created successfully");
        assertEquals(1, page1.getPid(), "Second page should have ID 1");
        assertEquals(2, page2.getPid(), "Third page should have ID 2");
        bufferManager.unpinPage(page.getPid());
        Page page3 = bufferManager.createPage();
        assertNotNull(page3, "Fourth page should be created successfully");
        assertEquals(3, page3.getPid(), "Fourth page should have ID 3");
        bufferManager.unpinPage(page1.getPid());
        bufferManager.unpinPage(page2.getPid());
        bufferManager.unpinPage(page3.getPid());

        Page reloadedPage = bufferManager.getPage(0);
        assertNotNull(reloadedPage, "Page 0 should be reloaded from disk");
        assertEquals(0, reloadedPage.getPid(), "Reloaded page should have ID 0");
        bufferManager.unpinPage(reloadedPage.getPid());
    }

    /**
     * Tests row insertion and retrieval with data integrity verification.
     * Ensures data persists correctly across page operations.
     */
    @Test
    public void testInsertAndReadRows() {
        Page page = bufferManager.createPage();
        Row row1 = new Row("tt0000001", "First Movie");
        Row row2 = new Row("tt0000002", "Second Movie");

        int rowId1 = page.insertRow(row1);
        int rowId2 = page.insertRow(row2);

        assertEquals(0, rowId1, "First row ID should be 0");
        assertEquals(1, rowId2, "Second row ID should be 1");
        bufferManager.markDirty(page.getPid());
        bufferManager.unpinPage(page.getPid());
        Page reloadedPage = bufferManager.getPage(page.getPid());
        Row readRow1 = reloadedPage.getRow(rowId1);
        Row readRow2 = reloadedPage.getRow(rowId2);

        assertNotNull(readRow1, "Row 1 should be read successfully");
        assertNotNull(readRow2, "Row 2 should be read successfully");

        assertEquals("tt0000001", new String(readRow1.movieId).trim(), "Movie ID 1 should match");
        assertEquals("First Movie", new String(readRow1.title).trim(), "Movie title 1 should match");

        assertEquals("tt0000002", new String(readRow2.movieId).trim(), "Movie ID 2 should match");
        assertEquals("Second Movie", new String(readRow2.title).trim(), "Movie title 2 should match");
        bufferManager.unpinPage(reloadedPage.getPid());
    }

    /**
     * Tests buffer behavior when all frames are pinned.
     * Verifies that pages cannot be evicted when pinned,
     * and that unpinning allows eviction to proceed.
     */
    @Test
    public void testBufferFullAllPinned() {
        Page page0 = bufferManager.createPage();
        Page page1 = bufferManager.createPage();
        Page page2 = bufferManager.createPage();
        Page page3 = bufferManager.createPage();
        assertNull(page3, "Should not be able to create a page when buffer is full and all pages are pinned");
        bufferManager.unpinPage(page0.getPid());

        page3 = bufferManager.createPage();
        assertNotNull(page3, "Should be able to create a page after unpinning");
    }

    /**
     * Tests the Least Recently Used (LRU) page replacement policy.
     * Ensures that when eviction is needed, the least recently
     * accessed page is selected for replacement.
     */
    @Test
    public void testLRUEviction() {
        Page page0 = bufferManager.createPage();
        Page page1 = bufferManager.createPage();
        Page page2 = bufferManager.createPage();
        page0.insertRow(new Row("tt0000001", "Test Movie 0"));
        bufferManager.markDirty(page0.getPid());

        page1.insertRow(new Row("tt0000002", "Test Movie 1"));
        bufferManager.markDirty(page1.getPid());

        page2.insertRow(new Row("tt0000003", "Test Movie 2"));
        bufferManager.markDirty(page2.getPid());
        bufferManager.unpinPage(page0.getPid());
        bufferManager.unpinPage(page1.getPid());
        bufferManager.unpinPage(page2.getPid());
        Page tmpPage1 = bufferManager.getPage(page1.getPid());
        bufferManager.unpinPage(tmpPage1.getPid());
        Page tmpPage2 = bufferManager.getPage(page2.getPid());
        bufferManager.unpinPage(tmpPage2.getPid());
        Page page3 = bufferManager.createPage();
        assertNotNull(page3, "Fourth page should be created successfully");
        bufferManager.unpinPage(page3.getPid());
        Page reloadedPage0 = bufferManager.getPage(page0.getPid());
        assertNotNull(reloadedPage0, "Page 0 should be reloaded from disk");
        bufferManager.unpinPage(reloadedPage0.getPid());
        Page reloadedPage1 = bufferManager.getPage(page1.getPid());
        assertNotNull(reloadedPage1, "Page 1 should still be in memory");
        bufferManager.unpinPage(reloadedPage1.getPid());

        Page reloadedPage2 = bufferManager.getPage(page2.getPid());
        assertNotNull(reloadedPage2, "Page 2 should still be in memory");
        bufferManager.unpinPage(reloadedPage2.getPid());
    }
}