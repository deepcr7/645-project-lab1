package buffermanager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;  
import java.util.*;

public class BufferManagerImpl extends BufferManager {
    
    private class Frame {
        Page page;
        boolean dirty;
        int pinCount;
        
        Frame(Page page) {
            this.page = page;
            this.dirty = false;
            this.pinCount = 1;
        }
    }
    
    private final String filename;
    private final Frame[] bufferPool;
    private final Map<Integer, Integer> pageTable;
    private final LinkedList<Integer> lruList;
    private int nextPageId;
    
    public BufferManagerImpl(int bufferSize, String filename) {
        super(bufferSize);
        this.filename = filename;
        this.bufferPool = new Frame[bufferSize];
        this.pageTable = new HashMap<>();
        this.lruList = new LinkedList<>();
        this.nextPageId = 0;
        
        try {
            File file = new File(filename);
            if (!file.exists()) {
                file.createNewFile();
            } else {
                this.nextPageId = (int) (file.length() / PageImpl.PAGE_SIZE);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize the database file: " + e.getMessage());
        }
    }
    
@Override
public Page getPage(int pageId) {
    
    // First, check if the page is already in buffer pool
    if (pageTable.containsKey(pageId)) {
        int frameId = pageTable.get(pageId);
        Frame frame = bufferPool[frameId];
        
        // Update LRU list (remove and add to end)
        lruList.remove(Integer.valueOf(frameId));
        lruList.add(frameId);
        
        // Increment pin count
        frame.pinCount++;
        
        return frame.page;
    }
    
    
    // Page not in buffer, need to load from disk
    // Find a free frame or evict a page
    int frameId = findFreeFrame();
    if (frameId == -1) {
        System.out.println("No free frames available for page " + pageId);
        return null;
    }
    
    
    // Load page from disk
    Page page = loadPageFromDisk(pageId);
    if (page == null) {
        System.out.println("Failed to load page " + pageId + " from disk");
        return null;
    }
    
    // Add page to buffer pool
    Frame newFrame = new Frame(page);
    bufferPool[frameId] = newFrame;
    pageTable.put(pageId, frameId);
    lruList.add(frameId);
    
    return page;
}
    
    @Override
    public Page createPage() {
        int pageId = nextPageId++;
        
        int frameId = findFreeFrame();
        if (frameId == -1) {
            return null;
        }
        
        Page page = new PageImpl(pageId);
        
        Frame newFrame = new Frame(page);
        bufferPool[frameId] = newFrame;
        pageTable.put(pageId, frameId);
        lruList.add(frameId);
        
        return page;
    }
    
    @Override
    public void markDirty(int pageId) {
        if (pageTable.containsKey(pageId)) {
            int frameId = pageTable.get(pageId);
            bufferPool[frameId].dirty = true;
        }
    }
    
    @Override
    public void unpinPage(int pageId) {
        if (pageTable.containsKey(pageId)) {
            int frameId = pageTable.get(pageId);
            if (bufferPool[frameId].pinCount > 0) {
                bufferPool[frameId].pinCount--;
            }
        }
    }
    
private int findFreeFrame() {
    // Look for an empty frame
    for (int i = 0; i < bufferSize; i++) {
        if (bufferPool[i] == null) {
            return i;
        }
    }
    
    
    // No empty frames, try to evict using LRU
    for (int frameId : lruList) {
        Frame frame = bufferPool[frameId];
        
        // Can only evict unpinned pages
        if (frame.pinCount == 0) {

            
            // Write dirty page to disk before eviction
            if (frame.dirty) {
                writePageToDisk(frame.page);
            }
            
            // Get the pageId before removing the frame
            int evictedPageId = frame.page.getPid();
            
            // Remove from page table and LRU list
            pageTable.remove(evictedPageId);
            lruList.remove(Integer.valueOf(frameId));
            
            // The frame is now free to use
            return frameId;
        }
    }
    
    // All frames are pinned
    return -1;
}
    
private void writePageToDisk(Page page) {
    try (RandomAccessFile file = new RandomAccessFile(filename, "rw")) {
        long offset = (long) page.getPid() * PageImpl.PAGE_SIZE;
        
        file.seek(offset);
        file.write(page.getData());
    } catch (IOException e) {
        System.err.println("Failed to write page to disk: " + e.getMessage());
        throw new RuntimeException("Failed to write page to disk: " + e.getMessage());
    }
}

private Page loadPageFromDisk(int pageId) {
    try (RandomAccessFile file = new RandomAccessFile(filename, "r")) {
        // Calculate file offset
        long offset = (long) pageId * PageImpl.PAGE_SIZE;
        
        // Check if offset is valid
        if (offset >= file.length()) {
            System.out.println("Cannot load page " + pageId + " from disk: offset " + offset + " >= file length " + file.length());
            return null;
        }
        
        // Read page data
        file.seek(offset);
        byte[] data = new byte[PageImpl.PAGE_SIZE];
        int bytesRead = file.read(data);
        
        // Check if we read the complete page
        if (bytesRead != PageImpl.PAGE_SIZE) {
            return null;
        }
        
        // Read page ID from data
        int storedPageId = ByteBuffer.wrap(data).getInt(0);
        
        // Create page from data
        Page page = new PageImpl(pageId, data);
        return page;
    } catch (IOException e) {
        System.err.println("Failed to load page from disk: " + e.getMessage());
        throw new RuntimeException("Failed to load page from disk: " + e.getMessage());
    }
}
    
    public void flushAll() {
        for (int i = 0; i < bufferSize; i++) {
            if (bufferPool[i] != null && bufferPool[i].dirty) {
                writePageToDisk(bufferPool[i].page);
                bufferPool[i].dirty = false;
            }
        }
    }
}