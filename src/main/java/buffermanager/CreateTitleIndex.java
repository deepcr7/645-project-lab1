package buffermanager;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.Map;

/**
 * Utility class to create the B+ tree indexes for query optimization.
 */
public class CreateTitleIndex {
    private static final String MOVIES_FILE = "imdb_movies.bin";

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java CreateTitleIndex <bufferSize>");
            return;
        }

        int bufferSize = Integer.parseInt(args[0]);
        System.out.println("Creating title index with buffer size: " + bufferSize);

        // Check if movies file exists
        File moviesFile = new File(MOVIES_FILE);
        if (!moviesFile.exists()) {
            System.err.println("Error: " + MOVIES_FILE + " not found. Run PreProcess first.");
            return;
        }
        System.out.println("Found movies file: " + moviesFile.getAbsolutePath() +
                " (size: " + moviesFile.length() + " bytes)");

        // Delete existing index file if it exists
        File indexFile = new File("imdb_title_index.bin");
        if (indexFile.exists()) {
            indexFile.delete();
            System.out.println("Deleted existing title index file.");
        }

        // Create index using TreeMap
        TreeMap<String, List<Rid>> index = new TreeMap<>();
        ExtendedBufferManagerImpl bufferManager = new ExtendedBufferManagerImpl(bufferSize);

        try {
            // Scan Movies table and build index
            System.out.println("Creating title index...");
            int pageId = 0;
            int totalRows = 0;
            while (true) {
                Page page = bufferManager.getPage(MOVIES_FILE, pageId);
                if (page == null) {
                    System.out.println("Reached end of file at page " + pageId);
                    break;
                }

                // Count rows in page
                int numRows = 0;
                while (page.getRow(numRows) != null) {
                    numRows++;
                }
                totalRows += numRows;
                System.out.println("Page " + pageId + " has " + numRows + " rows");

                for (int slotId = 0; slotId < numRows; slotId++) {
                    Row row = page.getRow(slotId);
                    if (row == null)
                        continue;

                    String title = new String(row.title).trim();
                    if (title.isEmpty())
                        continue;

                    Rid rid = new Rid(pageId, slotId);
                    index.computeIfAbsent(title, k -> new ArrayList<>()).add(rid);
                }

                bufferManager.unpinPage(MOVIES_FILE, pageId);
                pageId++;
            }

            // Save index to file
            System.out.println("Saving index to file...");
            System.out.println("Total pages processed: " + pageId);
            System.out.println("Total rows processed: " + totalRows);
            System.out.println("Index created with " + index.size() + " unique titles");
            System.out.println("Sample keys in index:");
            int count = 0;
            for (String key : index.keySet()) {
                if (count++ >= 5)
                    break;
                System.out.println("  '" + key + "' -> " + index.get(key).size() + " RIDs");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            bufferManager.force(MOVIES_FILE); // Force any remaining pages to disk
        }
    }
}