package buffermanager;

/**
 * Abstract class defining the interface for a buffer manager.
 * The buffer manager is responsible for loading pages from disk
 * and managing them in memory.
 */
public abstract class BufferManager {

    // Configurable size of buffer cache.
    final int bufferSize;
    
    /**
     * Creates a new buffer manager with the given buffer size.
     * 
     * @param bufferSize The number of pages that can be held in memory
     */
    public BufferManager(int bufferSize) {
        this.bufferSize = bufferSize;
    }
    
    /**
     * Fetches a page from memory if available; otherwise, loads it from disk.
     * The page is immediately pinned.
     * 
     * @param pageId The ID of the page to fetch.
     * @return The Page object whose content is stored in a frame of the buffer pool manager.
     */
    public abstract Page getPage(int pageId);

    /**
     * Creates a new page.
     * The page is immediately pinned.
     * 
     * @return The Page object whose content is stored in a frame of the buffer pool manager.
     */
    public abstract Page createPage();

    /**
     * Marks a page as dirty, indicating it needs to be written to disk before eviction.
     * 
     * @param pageId The ID of the page to mark as dirty.
     */
    public abstract void markDirty(int pageId);

    /**
     * Unpins a page in the buffer pool, allowing it to be evicted if necessary.
     * 
     * @param pageId The ID of the page to unpin.
     */
    public abstract void unpinPage(int pageId);
}