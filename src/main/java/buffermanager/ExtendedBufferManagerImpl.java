package buffermanager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

public class ExtendedBufferManagerImpl extends ExtendedBufferManager {

    private class Frame {
        Page page;
        String filename;
        boolean dirty;
        int pinCount;

        Frame(Page page, String filename) {
            this.page = page;
            this.filename = filename;
            this.dirty = false;
            this.pinCount = 1;
        }
    }

    private final Frame[] bufferPool;
    private final Map<String, Map<Integer, Integer>> filePageTable;
    private final LinkedList<Integer> lruList;
    private final Map<String, Integer> nextPageIds;

    public ExtendedBufferManagerImpl(int bufferSize) {
        super(bufferSize);
        this.bufferPool = new Frame[bufferSize];
        this.filePageTable = new HashMap<>();
        this.lruList = new LinkedList<>();
        this.nextPageIds = new HashMap<>();
    }

    @Override
    public Page getPage(int pageId) {
        throw new UnsupportedOperationException("Use getPage(String filename, int pageId) instead");
    }

    @Override
    public Page createPage() {
        throw new UnsupportedOperationException("Use createPage(String filename) instead");
    }

    @Override
    public void markDirty(int pageId) {
        throw new UnsupportedOperationException("Use markDirty(String filename, int pageId) instead");
    }

    @Override
    public void unpinPage(int pageId) {
        throw new UnsupportedOperationException("Use unpinPage(String filename, int pageId) instead");
    }

    @Override
    public Page getPage(String filename, int pageId) {
        // Initialize file page table for this file if not exists
        if (!filePageTable.containsKey(filename)) {
            filePageTable.put(filename, new HashMap<>());
            initializeFile(filename);
        }

        Map<Integer, Integer> pageTable = filePageTable.get(filename);

        if (pageTable.containsKey(pageId)) {
            int frameId = pageTable.get(pageId);
            Frame frame = bufferPool[frameId];

            lruList.remove(Integer.valueOf(frameId));
            lruList.add(frameId);

            frame.pinCount++;

            return frame.page;
        }

        int frameId = findFreeFrame();
        if (frameId == -1) {

            return null;
        }

        Page page = loadPageFromDisk(filename, pageId);
        if (page == null) {
            return null;
        }

        Frame newFrame = new Frame(page, filename);
        bufferPool[frameId] = newFrame;
        pageTable.put(pageId, frameId);
        lruList.add(frameId);

        return page;
    }

    @Override
    public Page createPage(String filename) {
        if (!filePageTable.containsKey(filename)) {
            filePageTable.put(filename, new HashMap<>());
            initializeFile(filename);
        }

        Map<Integer, Integer> pageTable = filePageTable.get(filename);

        int pageId = nextPageIds.get(filename);
        nextPageIds.put(filename, pageId + 1);

        int frameId = findFreeFrame();
        if (frameId == -1) {
            return null;
        }

        Page page = new PageImpl(pageId);

        Frame newFrame = new Frame(page, filename);
        bufferPool[frameId] = newFrame;
        pageTable.put(pageId, frameId);
        lruList.add(frameId);

        return page;
    }

    @Override
    public void markDirty(String filename, int pageId) {
        if (!filePageTable.containsKey(filename)) {
            return;
        }

        Map<Integer, Integer> pageTable = filePageTable.get(filename);
        if (pageTable.containsKey(pageId)) {
            int frameId = pageTable.get(pageId);
            bufferPool[frameId].dirty = true;
        }
    }

    @Override
    public void unpinPage(String filename, int pageId) {
        if (!filePageTable.containsKey(filename)) {
            return;
        }

        Map<Integer, Integer> pageTable = filePageTable.get(filename);
        if (pageTable.containsKey(pageId)) {
            int frameId = pageTable.get(pageId);
            if (bufferPool[frameId] != null && bufferPool[frameId].pinCount > 0) {
                bufferPool[frameId].pinCount--;
            }
        }
    }

    @Override
    public void force(String filename) {
        for (int i = 0; i < bufferSize; i++) {
            if (bufferPool[i] != null && bufferPool[i].dirty && bufferPool[i].filename.equals(filename)) {
                writePageToDisk(bufferPool[i].filename, bufferPool[i].page);
                bufferPool[i].dirty = false;
            }
        }
    }

    public void force() {
        for (int i = 0; i < bufferSize; i++) {
            if (bufferPool[i] != null && bufferPool[i].dirty) {
                writePageToDisk(bufferPool[i].filename, bufferPool[i].page);
                bufferPool[i].dirty = false;
            }
        }
    }

    private int findFreeFrame() {
        for (int i = 0; i < bufferSize; i++) {
            if (bufferPool[i] == null) {
                return i;
            }
        }

        for (Integer frameId : lruList) {
            if (frameId != null && frameId < bufferPool.length) {
                Frame frame = bufferPool[frameId];

                if (frame != null && frame.pinCount == 0) {
                    if (frame.dirty) {
                        writePageToDisk(frame.filename, frame.page);
                    }

                    int evictedPageId = frame.page.getPid();
                    String filename = frame.filename;

                    if (filePageTable.containsKey(filename)) {
                        filePageTable.get(filename).remove(evictedPageId);
                    }
                    lruList.remove(frameId);

                    return frameId;
                }
            }
        }

        // All frames are pinned
        return -1;
    }

    private void writePageToDisk(String filename, Page page) {
        try (RandomAccessFile file = new RandomAccessFile(filename, "rw")) {
            long offset = (long) page.getPid() * PageImpl.PAGE_SIZE;

            file.seek(offset);
            file.write(page.getData());
        } catch (IOException e) {
            System.err.println("Failed to write page to disk: " + e.getMessage());
        }
    }

    private Page loadPageFromDisk(String filename, int pageId) {
        try (RandomAccessFile file = new RandomAccessFile(filename, "r")) {
            // Calculate file offset
            long offset = (long) pageId * PageImpl.PAGE_SIZE;

            if (offset < 0) {
                System.err.println("Failed to load page from disk: Negative seek offset");
                return null;
            }

            if (offset >= file.length()) {
                return null;
            }

            file.seek(offset);
            byte[] data = new byte[PageImpl.PAGE_SIZE];
            int bytesRead = file.read(data);

            if (bytesRead != PageImpl.PAGE_SIZE) {
                return null;
            }

            return new PageImpl(pageId, data);
        } catch (IOException e) {
            System.err.println("Failed to load page from disk: " + e.getMessage());
            return null;
        }
    }

    private void initializeFile(String filename) {
        try {
            File file = new File(filename);
            if (!file.exists()) {
                file.createNewFile();
                nextPageIds.put(filename, 0);
            } else {
                nextPageIds.put(filename, (int) (file.length() / PageImpl.PAGE_SIZE));
            }
        } catch (IOException e) {
            System.err.println("Failed to initialize file: " + e.getMessage());
        }
    }

    public void freeUpBufferSpace() {

        int pinnedBefore = 0;
        for (int i = 0; i < bufferSize; i++) {
            if (bufferPool[i] != null && bufferPool[i].pinCount > 0) {
                pinnedBefore++;
            }
        }

        // First, identify pages with high pin counts
        for (String filename : filePageTable.keySet()) {
            Map<Integer, Integer> pageTable = filePageTable.get(filename);
            for (Integer pageId : pageTable.keySet()) {
                Integer frameId = pageTable.get(pageId);
                if (frameId != null && frameId < bufferPool.length) {
                    Frame frame = bufferPool[frameId];
                    if (frame != null && frame.pinCount > 2) {
                        // Reduce pin count to 1 for highly pinned pages
                        frame.pinCount = 1;
                    }
                }
            }
        }

        // Flush all dirty pages to disk
        force();

        int freeFrames = 0;
        int pinnedAfter = 0;
        for (int i = 0; i < bufferSize; i++) {
            if (bufferPool[i] == null) {
                freeFrames++;
            } else if (bufferPool[i].pinCount > 0) {
                pinnedAfter++;
            }
        }

    }

    public void aggressiveCleanup() {
        // Force flush all dirty pages
        force();

        // Reset pin counts for all pages
        for (int i = 0; i < bufferSize; i++) {
            if (bufferPool[i] != null && bufferPool[i].pinCount > 0) {
                bufferPool[i].pinCount = 1;
                unpinPage(bufferPool[i].filename, bufferPool[i].page.getPid());
            }
        }

        for (int i = 0; i < 100; i++) {
            findFreeFrame();
        }
    }
}