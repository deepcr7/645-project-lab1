package buffermanager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Utility class for loading data from the IMDB dataset.
 */
public class Utilities {
    
    /**
     * Loads the IMDB dataset into the buffer manager.
     * 
     * @param bufferManager The buffer manager to load data into
     * @param filepath The path to the title.basics.tsv file
     * @return The number of rows loaded
     */
    public static int loadDataset(BufferManager bufferManager, String filepath) {
        int rowsLoaded = 0;
        
        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            String line = reader.readLine();
            
            // Read and process each line
            Page currentPage = bufferManager.createPage();
            if (currentPage == null) {
                throw new RuntimeException("Failed to create initial page");
            }
            
            int currentPageId = currentPage.getPid();
            Random random = new Random(123);
            
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\t");
                
                // Get tconst (movie ID) and primaryTitle
                if (fields.length < 3) {
                    continue;
                }
                
                String movieId = fields[0];
                
                // Skip rows where movieId is longer than 9 bytes
                if (movieId.getBytes().length > 9) {
                    continue;
                }
                
                String title = fields[2];
                
                if (title.length() > 30) {
                    title = title.substring(0, 30);
                }
                
                // Create a row
                Row row = new Row(movieId, title);
                
                // Insert row into current page
                int rowId = currentPage.insertRow(row);
                
                // If page is full, write it to disk and create a new page
                if (rowId == -1) {
                    // Mark current page as dirty and unpin it
                    bufferManager.markDirty(currentPageId);
                    bufferManager.unpinPage(currentPageId);
                    
                    // Create a new page
                    currentPage = bufferManager.createPage();
                    if (currentPage == null) {
                        throw new RuntimeException("Failed to create a new page");
                    }
                    
                    currentPageId = currentPage.getPid();
                    
                    // Try inserting again into the new page
                    rowId = currentPage.insertRow(row);
                    if (rowId == -1) {
                        throw new RuntimeException("Failed to insert row into new page");
                    }
                }
                
                rowsLoaded++;
                
                // Interleave reads with inserts
                if (random.nextInt(100) < 5) {
                    // Randomly read a page that's already been created
                    if (currentPageId > 0) {
                        int randomPageId = random.nextInt(currentPageId);
                        
                        // Unpin current page first to avoid running out of buffer frames
                        bufferManager.unpinPage(currentPageId);
                        
                        // Get a random page
                        Page randomPage = bufferManager.getPage(randomPageId);
                        if (randomPage != null) {
                            // Read a random row
                            int randomRowId = random.nextInt(10); 
                            Row randomRow = randomPage.getRow(randomRowId);
                            // Unpin the page when done
                            bufferManager.unpinPage(randomPageId);
                        }
                        
                        // Get the current page back
                        currentPage = bufferManager.getPage(currentPageId);
                        if (currentPage == null) {
                            throw new RuntimeException("Failed to get back current page");
                        }
                        
                        // Mark dirty as we're going to insert more rows
                        bufferManager.markDirty(currentPageId);
                    }
                }
            }
            if (currentPage != null) {
                bufferManager.markDirty(currentPageId);
                bufferManager.unpinPage(currentPageId);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load dataset: " + e.getMessage());
        }
        
        return rowsLoaded;
    }
}