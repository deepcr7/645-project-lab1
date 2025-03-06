package buffermanager;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Implementation of the Page interface using a packed format to store rows.
 */
public class PageImpl implements Page {
    // Page size is 4KB (4096 bytes)
    public static final int PAGE_SIZE = 4096;
    
    // Header size: 4 bytes for page ID + 4 bytes for row count = 8 bytes
    private static final int HEADER_SIZE = 8;
    
    // Each row in our Movies table has:
    // - 9 bytes for movieId
    // - 30 bytes for title
    // Total: 39 bytes per row
    private static final int ROW_SIZE = 39;
    
    // Calculate max rows per page
    // We subtract HEADER_SIZE from PAGE_SIZE to account for the header
    private static final int MAX_ROWS = (PAGE_SIZE - HEADER_SIZE) / ROW_SIZE;
    
    // The page data as a byte array
    private byte[] data;
    
    // The page ID
    private int pageId;
    
    /**
     * Creates a new page with the given page ID.
     * 
     * @param pageId The page ID
     */
    public PageImpl(int pageId) {
        this.pageId = pageId;
        this.data = new byte[PAGE_SIZE];
        
        // Initialize the page header
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putInt(pageId);    // Page ID
        buffer.putInt(0);         // Initial row count is 0
    }
    
    /**
     * Creates a page from existing data.
     * 
     * @param pageId The page ID
     * @param data The raw page data
     */
    public PageImpl(int pageId, byte[] data) {
        this.pageId = pageId;
        this.data = Arrays.copyOf(data, PAGE_SIZE);
    }
    
    @Override
    public Row getRow(int rowId) {
        // Check if rowId is valid
        int rowCount = getRowCount();
        if (rowId < 0 || rowId >= rowCount) {
            return null;
        }
        
        // Calculate offset to the row
        int offset = HEADER_SIZE + rowId * ROW_SIZE;
        
        // Extract movieId (9 bytes)
        byte[] movieId = new byte[9];
        System.arraycopy(data, offset, movieId, 0, movieId.length);
        offset += movieId.length;
        
        // Extract title (30 bytes)
        byte[] title = new byte[30];
        System.arraycopy(data, offset, title, 0, title.length);
        
        return new Row(movieId, title);
    }
    
    @Override
    public int insertRow(Row row) {
        // Check if page is full
        int rowCount = getRowCount();
        if (rowCount >= MAX_ROWS) {
            return -1;
        }
        
        // Calculate offset for new row
        int offset = HEADER_SIZE + rowCount * ROW_SIZE;
        
        // Copy movieId
        System.arraycopy(row.movieId, 0, data, offset, row.movieId.length);
        offset += row.movieId.length;
        
        // Copy title
        System.arraycopy(row.title, 0, data, offset, row.title.length);
        
        // Increment row count in header
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.position(4);  // Skip page ID
        buffer.putInt(rowCount + 1);
        
        return rowCount;
    }
    
    @Override
    public boolean isFull() {
        return getRowCount() >= MAX_ROWS;
    }
    
    @Override
    public int getPid() {
        return pageId;
    }
    
    @Override
    public byte[] getData() {
        return Arrays.copyOf(data, PAGE_SIZE);
    }
    
    @Override
    public void setData(byte[] data) {
        this.data = Arrays.copyOf(data, PAGE_SIZE);
        // Update the pageId from the data
        ByteBuffer buffer = ByteBuffer.wrap(this.data);
        this.pageId = buffer.getInt();
    }
    
    /**
     * Gets the number of rows stored in this page.
     * 
     * @return The row count
     */
    private int getRowCount() {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.position(4);  // Skip page ID
        return buffer.getInt();
    }
}