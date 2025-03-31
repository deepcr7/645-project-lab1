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
     * @param filepath      The path to the title.basics.tsv file
     * @return The number of rows loaded
     */
    public static int loadDataset(BufferManager bufferManager, String filepath) {
        int rowsLoaded = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            String line = reader.readLine();
            Page currentPage = bufferManager.createPage();
            if (currentPage == null) {
                throw new RuntimeException("Failed to create initial page");
            }

            int currentPageId = currentPage.getPid();
            Random random = new Random(123);

            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\t");
                if (fields.length < 3) {
                    continue;
                }

                String movieId = fields[0];
                if (movieId.getBytes().length > 9) {
                    continue;
                }

                String title = fields[2];

                if (title.length() > 30) {
                    title = title.substring(0, 30);
                }
                Row row = new Row(movieId, title);
                int rowId = currentPage.insertRow(row);
                if (rowId == -1) {
                    bufferManager.markDirty(currentPageId);
                    bufferManager.unpinPage(currentPageId);
                    currentPage = bufferManager.createPage();
                    if (currentPage == null) {
                        throw new RuntimeException("Failed to create a new page");
                    }

                    currentPageId = currentPage.getPid();
                    rowId = currentPage.insertRow(row);
                    if (rowId == -1) {
                        throw new RuntimeException("Failed to insert row into new page");
                    }
                }

                rowsLoaded++;
                if (random.nextInt(100) < 5) {
                    if (currentPageId > 0) {
                        int randomPageId = random.nextInt(currentPageId);
                        bufferManager.unpinPage(currentPageId);
                        Page randomPage = bufferManager.getPage(randomPageId);
                        if (randomPage != null) {
                            int randomRowId = random.nextInt(10);
                            Row randomRow = randomPage.getRow(randomRowId);
                            bufferManager.unpinPage(randomPageId);
                        }
                        currentPage = bufferManager.getPage(currentPageId);
                        if (currentPage == null) {
                            throw new RuntimeException("Failed to get back current page");
                        }
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

    /**
     * Loads the IMDB dataset into the extended buffer manager.
     * 
     * @param bufferManager The extended buffer manager to load data into
     * @param dataFile      The name of the data file
     * @param filepath      The path to the title.basics.tsv file
     * @return The number of rows loaded
     */
    public static int loadDataset(ExtendedBufferManager bufferManager, String dataFile, String filepath) {
        int rowsLoaded = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            String line = reader.readLine();
            Page currentPage = bufferManager.createPage(dataFile);
            if (currentPage == null) {
                throw new RuntimeException("Failed to create initial page");
            }

            int currentPageId = currentPage.getPid();
            Random random = new Random(123);

            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\t");
                if (fields.length < 3) {
                    continue;
                }

                String movieId = fields[0];
                if (movieId.getBytes().length > 9) {
                    continue;
                }

                String title = fields[2];

                if (title.length() > 30) {
                    title = title.substring(0, 30);
                }
                Row row = new Row(movieId, title);
                int rowId = currentPage.insertRow(row);
                if (rowId == -1) {
                    bufferManager.markDirty(dataFile, currentPageId);
                    bufferManager.unpinPage(dataFile, currentPageId);
                    currentPage = bufferManager.createPage(dataFile);
                    if (currentPage == null) {
                        throw new RuntimeException("Failed to create a new page");
                    }

                    currentPageId = currentPage.getPid();
                    rowId = currentPage.insertRow(row);
                    if (rowId == -1) {
                        throw new RuntimeException("Failed to insert row into new page");
                    }
                }

                rowsLoaded++;
                if (random.nextInt(100) < 5) {
                    if (currentPageId > 0) {
                        int randomPageId = random.nextInt(currentPageId);
                        bufferManager.unpinPage(dataFile, currentPageId);
                        Page randomPage = bufferManager.getPage(dataFile, randomPageId);
                        if (randomPage != null) {
                            int randomRowId = random.nextInt(10);
                            Row randomRow = randomPage.getRow(randomRowId);
                            bufferManager.unpinPage(dataFile, randomPageId);
                        }
                        currentPage = bufferManager.getPage(dataFile, currentPageId);
                        if (currentPage == null) {
                            throw new RuntimeException("Failed to get back current page");
                        }
                        bufferManager.markDirty(dataFile, currentPageId);
                    }
                }
            }
            if (currentPage != null) {
                bufferManager.markDirty(dataFile, currentPageId);
                bufferManager.unpinPage(dataFile, currentPageId);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load dataset: " + e.getMessage());
        }

        return rowsLoaded;
    }
}