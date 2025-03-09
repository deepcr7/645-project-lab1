package buffermanager;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class PageImplTest {
    
    @Test
    public void testInsertAndGetRow() {
        // Create a new page
        PageImpl page = new PageImpl(1);
        
        // Create test rows
        Row row1 = new Row("tt0000001", "Test Movie 1");
        Row row2 = new Row("tt0000002", "Test Movie 2");
        
        // Insert rows
        int rowId1 = page.insertRow(row1);
        int rowId2 = page.insertRow(row2);
        
        // Check row IDs
        assertEquals(0, rowId1, "First row should have ID 0");
        assertEquals(1, rowId2, "Second row should have ID 1");
        
        // Retrieve rows
        Row retrievedRow1 = page.getRow(rowId1);
        Row retrievedRow2 = page.getRow(rowId2);
        
        // Check row contents
        assertNotNull(retrievedRow1, "Retrieved row 1 should not be null");
        assertNotNull(retrievedRow2, "Retrieved row 2 should not be null");
        
        assertEquals("tt0000001", new String(retrievedRow1.movieId).trim(), "Movie ID should match");
        assertEquals("Test Movie 1", new String(retrievedRow1.title).trim(), "Movie title should match");
        
        assertEquals("tt0000002", new String(retrievedRow2.movieId).trim(), "Movie ID should match");
        assertEquals("Test Movie 2", new String(retrievedRow2.title).trim(), "Movie title should match");
    }
    
@Test
public void testPageFull() {
    // Create a new page
    PageImpl page = new PageImpl(1);
    
    // Calculate how many rows should fit in a page
    int rowSize = 9 + 30;
    int headerSize = 8;
    int maxRows = (PageImpl.PAGE_SIZE - headerSize) / rowSize;
    
    // Insert rows until one less than full
    for (int i = 0; i < maxRows - 1; i++) {
        Row row = new Row("tt" + String.format("%07d", i), "Movie " + i);
        int rowId = page.insertRow(row);
        assertEquals(i, rowId, "Row ID should match insertion order");
        assertFalse(page.isFull(), "Page should not be full until last row");
    }
    
    // Insert the last row that should make the page full
    Row lastRow = new Row("tt" + String.format("%07d", maxRows - 1), "Movie " + (maxRows - 1));
    int lastRowId = page.insertRow(lastRow);
    assertEquals(maxRows - 1, lastRowId, "Last row ID should match insertion order");
    assertTrue(page.isFull(), "Page should be full after inserting maximum rows");
    
    // Try to insert one more row
    Row extraRow = new Row("ttEXTRA", "Extra Movie");
    int rowId = page.insertRow(extraRow);
    assertEquals(-1, rowId, "Should return -1 when inserting into a full page");
}
    
    @Test
    public void testGetNonExistentRow() {
        // Create a new page
        PageImpl page = new PageImpl(1);
        
        // Try to get a row that doesn't exist
        Row row = page.getRow(0);
        assertNull(row, "Should return null for non-existent row");
        
        // Insert a row
        page.insertRow(new Row("tt0000001", "Test Movie"));
        
        // Try to get a row beyond the row count
        row = page.getRow(1);
        assertNull(row, "Should return null for row beyond row count");
    }
    
    @Test
    public void testGetPageId() {
        int testPageId = 42;
        PageImpl page = new PageImpl(testPageId);
        assertEquals(testPageId, page.getPid(), "Page ID should match constructor argument");
    }
    
@Test
public void testGetAndSetData() {
    // Create a page
    int testPageId = 5;
    PageImpl page = new PageImpl(testPageId);
    
    // Insert some data
    page.insertRow(new Row("tt0000001", "Test Movie"));
    
    // Get the data
    byte[] data = page.getData();
    
    // Create a new page with the data
    PageImpl newPage = new PageImpl(0, data);
    
    // Check that the page ID was correctly restored from the data
    assertEquals(testPageId, newPage.getPid(), "Page ID should be restored from data");
    
    // Check that the row was correctly restored
    Row row = newPage.getRow(0);
    assertNotNull(row, "Row should be restored from data");
    assertEquals("tt0000001", new String(row.movieId).trim(), "Movie ID should be restored");
    assertEquals("Test Movie", new String(row.title).trim(), "Movie title should be restored");
}
}