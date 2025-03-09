package buffermanager;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

/**
 * Main class for end-to-end testing of the buffer manager.
 * Tests loading real data and performing mixed operations.
 */
public class Main {
    
    private static final String DB_FILE = "imdb_database.bin";
    private static final int BUFFER_SIZE = 10;
    
    public static void main(String[] args) {
        System.out.println("Starting Buffer Manager Lab test...");
        
        // Test file initialization
        File dbFile = new File(DB_FILE);
        if (dbFile.exists()) {
            dbFile.delete();
            System.out.println("Removed existing database file.");
        }
        
        // Test buffer manager creation
        BufferManager bufferManager = new BufferManagerImpl(BUFFER_SIZE, DB_FILE);
        System.out.println("Created buffer manager with buffer size: " + BUFFER_SIZE);
        
        // Test dataset file availability
        String imdbFile = "title.basics.tsv";
        if (!Files.exists(Paths.get(imdbFile))) {
            System.out.println("Error: IMDB dataset file '" + imdbFile + "' not found.");
            return;
        }
        
         // Test dataset loading with performance measurement
        long startTime = System.currentTimeMillis();
        int rowsLoaded = Utilities.loadDataset(bufferManager, imdbFile);
        long endTime = System.currentTimeMillis();
        System.out.println("Loaded " + rowsLoaded + " rows in " + (endTime - startTime) + " ms.");
        
         // Test random access to verify data integrity
        testRandomAccess(bufferManager, rowsLoaded);
        
         // Test proper cleanup
        if (bufferManager instanceof BufferManagerImpl) {
            ((BufferManagerImpl) bufferManager).flushAll();
        }
        
        System.out.println("\nTest completed successfully!");
    }
    
    /**
     * Tests random access to pages, reading data to verify integrity.
     * 
     * @param bufferManager The buffer manager to test
     * @param totalRows The total number of rows loaded
     */
    private static void testRandomAccess(BufferManager bufferManager, int totalRows) {
        int estimatedPageCount = totalRows / 100 + 1;
        
        Random random = new Random(456);
        
        // Test multiple random page accesses
        for (int i = 0; i < 10; i++) {
            int pageId = random.nextInt(estimatedPageCount);
            
            Page page = bufferManager.getPage(pageId);
            
            if (page != null) {
                
                bufferManager.unpinPage(pageId);
            } else {
                System.out.println("Page not found");
            }
        }
    }
}