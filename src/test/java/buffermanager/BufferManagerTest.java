package buffermanager;



import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class BufferManagerTest {

    private BufferManagerImpl bufferManager;

    @BeforeEach
    public void setUp() {
        bufferManager = new BufferManagerImpl(5, "test.db");
    }

    @Test
    public void testGetPage_existingPage() {
        Page page1 = bufferManager.createPage();
        Page page2 = bufferManager.createPage();

        Page retrievedPage1 = bufferManager.getPage(page1.getPid());
        Page retrievedPage2 = bufferManager.getPage(page2.getPid());

        assertNotNull(retrievedPage1);
        assertNotNull(retrievedPage2);
        assertEquals(page1, retrievedPage1);
        assertEquals(page2, retrievedPage2);
    }

    @Test
    public void testGetPage_nonExistingPage() {
        Page retrievedPage = bufferManager.getPage(100);

        assertNull(retrievedPage);
    }

    @Test
    public void testCreatePage() {
        Page page = bufferManager.createPage();
        assertNotNull(page);
    }

    @Test
    public void testMarkDirty_existingPage() {
        Page page = bufferManager.createPage();
        bufferManager.markDirty(page.getPid());

        // Assert that the page is marked as dirty
        assertTrue(bufferManager.getBufferPool()[0].dirty);
    }

    @Test
    public void testMarkDirty_nonExistingPage() {
        bufferManager.markDirty(100);

        // Assert that nothing is marked as dirty
        assertNull(bufferManager.getBufferPool()[0]);
    }

    @Test
    public void testUnpinPage_existingPage() {
        Page page = bufferManager.createPage();
        bufferManager.unpinPage(page.getPid());

        // Assert that the pin count is decremented
        assertEquals(0, bufferManager.getBufferPool()[0].pinCount);
    }

    @Test
    public void testUnpinPage_nonExistingPage() {
        bufferManager.unpinPage(100);

        // Assert that nothing is unpinned
        assertNull(bufferManager.getBufferPool()[0]);
    }

    @Test
    public void testFlushAll_dirtyPages() {
        Page page1 = bufferManager.createPage();
        Page page2 = bufferManager.createPage();

        bufferManager.markDirty(page1.getPid());
        bufferManager.markDirty(page2.getPid());

        bufferManager.flushAll();

        // Assert that the dirty pages are flushed
        assertFalse(bufferManager.getBufferPool()[0].dirty);
        assertFalse(bufferManager.getBufferPool()[1].dirty);
    }
}