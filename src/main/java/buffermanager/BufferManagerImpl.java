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
        Page page = loadPageFromDisk(pageId);
        if (page == null) {
            return null;
        }
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
        for (int i = 0; i < bufferSize; i++) {
            if (bufferPool[i] == null) {
                return i;
            }
        }

        for (int frameId : lruList) {
            Frame frame = bufferPool[frameId];

            if (frame.pinCount == 0) {

                if (frame.dirty) {
                    writePageToDisk(frame.page);
                }

                int evictedPageId = frame.page.getPid();

                pageTable.remove(evictedPageId);
                lruList.remove(Integer.valueOf(frameId));

                return frameId;
            }
        }

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
            long offset = (long) pageId * PageImpl.PAGE_SIZE;
            if (offset >= file.length()) {
                return null;
            }

            file.seek(offset);
            byte[] data = new byte[PageImpl.PAGE_SIZE];
            int bytesRead = file.read(data);

            if (bytesRead != PageImpl.PAGE_SIZE) {
                return null;
            }

            int storedPageId = ByteBuffer.wrap(data).getInt(0);

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

    public void force() {
        flushAll();
    }
}