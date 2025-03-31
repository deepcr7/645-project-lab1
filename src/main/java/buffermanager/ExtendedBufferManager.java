package buffermanager;

public abstract class ExtendedBufferManager extends BufferManager {
    public ExtendedBufferManager(int bufferSize) {
        super(bufferSize);
    }
    
    public abstract Page getPage(String filename, int pageId);
    
    public abstract Page createPage(String filename);
    
    public abstract void markDirty(String filename, int pageId);
    
    public abstract void unpinPage(String filename, int pageId);
    
    public abstract void force(String filename);
}