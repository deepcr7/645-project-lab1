package buffermanager;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Implementation of the Page interface for the WorkedOn table.
 */
public class WorkedOnPageImpl implements Page {
    // Page size is 4KB (4096 bytes)
    public static final int PAGE_SIZE = 4096;

    // Header size: 4 bytes for page ID + 4 bytes for row count = 8 bytes
    private static final int HEADER_SIZE = 8;

    // Each row in our WorkedOn table has:
    // - 9 bytes for movieId
    // - 10 bytes for personId
    // - 20 bytes for category
    // Total: 39 bytes per row
    private static final int ROW_SIZE = 39;

    // Calculate max rows per page
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
    public WorkedOnPageImpl(int pageId) {
        this.pageId = pageId;
        this.data = new byte[PAGE_SIZE];

        // Initialize the page header
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putInt(pageId); // Page ID
        buffer.putInt(0); // Initial row count is 0
    }

    /**
     * Creates a page from existing data.
     * 
     * @param pageId The page ID
     * @param data   The raw page data
     */
    public WorkedOnPageImpl(int pageId, byte[] data) {
        this.pageId = pageId;
        this.data = Arrays.copyOf(data, PAGE_SIZE);
    }

    @Override
    public Row getRow(int rowId) {
        // This method is not used directly for WorkedOn
        return null;
    }

    /**
     * Fetches a WorkedOn row from the page by its row ID.
     * 
     * @param rowId The ID of the row to retrieve.
     * @return The WorkedOnRow object containing the requested data.
     */
    public WorkedOnRow getWorkedOnRow(int rowId) {
        // Check if rowId is valid
        int rowCount = getRowCount();
        if (rowId < 0 || rowId >= rowCount) {
            return null;
        }

        int offset = HEADER_SIZE + rowId * ROW_SIZE;

        if (offset + ROW_SIZE > PAGE_SIZE) {
            return null;
        }

        // Extract movieId (9 bytes)
        byte[] movieId = new byte[9];
        System.arraycopy(data, offset, movieId, 0, movieId.length);
        offset += movieId.length;

        // Extract personId (10 bytes)
        byte[] personId = new byte[10];
        System.arraycopy(data, offset, personId, 0, personId.length);
        offset += personId.length;

        // Extract category (20 bytes)
        byte[] category = new byte[20];
        System.arraycopy(data, offset, category, 0, category.length);

        return new WorkedOnRow(movieId, personId, category);
    }

    @Override
    public int insertRow(Row row) {
        // This method is not used directly for WorkedOn
        return -1;
    }

    /**
     * Inserts a new WorkedOn row into the page.
     * 
     * @param row The WorkedOnRow object containing the data to insert.
     * @return The row ID of the inserted row, or -1 if the page is full
     */
    public int insertWorkedOnRow(WorkedOnRow row) {
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

        // Copy personId
        System.arraycopy(row.personId, 0, data, offset, row.personId.length);
        offset += row.personId.length;

        // Copy category
        System.arraycopy(row.category, 0, data, offset, row.category.length);

        // Increment row count in header
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.position(4); // Skip page ID
        buffer.putInt(rowCount + 1);

        return rowCount;
    }

    @Override
    public boolean isFull() {
        int rowCount = getRowCount();
        return rowCount >= MAX_ROWS;
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
    }

    private int getRowCount() {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.position(4); // Skip page ID
        return buffer.getInt();
    }
}