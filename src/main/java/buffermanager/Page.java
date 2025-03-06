package buffermanager;

/**
 * Interface for a database page that stores rows using a packed format.
 */
public interface Page {
    /**
     * Fetches a row from the page by its row ID.
     * 
     * @param rowId The ID of the row to retrieve.
     * @return The Row object containing the requested data.
     */
    Row getRow(int rowId);

    /**
     * Inserts a new row into the page.
     * 
     * @param row The Row object containing the data to insert.
     * @return The row ID of the inserted row, or -1 if the page is full
     */
    int insertRow(Row row);

    /**
     * Check if the page is full.
     * 
     * @return true if the page is full, false otherwise 
     */
    boolean isFull();
    
    /**
     * Gets the page ID for this page.
     * 
     * @return The page ID
     */
    int getPid();
    
    /**
     * Gets the raw byte data for this page.
     * 
     * @return The byte array containing the page data
     */
    byte[] getData();
    
    /**
     * Sets the raw byte data for this page.
     * 
     * @param data The byte array containing the page data
     */
    void setData(byte[] data);
}