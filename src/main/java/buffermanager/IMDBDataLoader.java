package buffermanager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

/**
 * Utility class for loading IMDB data into the different tables.
 */
public class IMDBDataLoader {
    private static final String MOVIES_FILE = "imdb_movies.bin";
    private static final String WORKEDON_FILE = "imdb_workedon.bin";
    private static final String PEOPLE_FILE = "imdb_people.bin";

    public static int loadWorkedOnData(ExtendedBufferManager bufferManager, String filepath) {
        int rowsLoaded = 0;
        int directorCount = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            // Skip header line
            String line = reader.readLine();
            System.out.println("WorkedOn header: " + line);

            // Get a page from the buffer manager
            Page page = bufferManager.createPage(WORKEDON_FILE);
            if (page == null) {
                throw new RuntimeException("Failed to create initial page for WorkedOn");
            }

            int currentPageId = page.getPid();
            WorkedOnPageImpl workedOnPage = new WorkedOnPageImpl(currentPageId);

            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\t");
                if (fields.length < 4) {
                    continue;
                }

                String movieId = fields[0]; // tconst
                if (movieId.length() > 9) {
                    continue;
                }

                String personId = fields[2]; // nconst
                if (personId.length() > 10) {
                    continue;
                }

                String category = fields[3]; // category
                if (category.length() > 20) {
                    category = category.substring(0, 20);
                }

                // Debug info for directors
                if (category.toLowerCase().contains("direct")) {
                    directorCount++;
                }

                // Insert row into page
                WorkedOnRow row = new WorkedOnRow(movieId, personId, category);
                int rowId = workedOnPage.insertWorkedOnRow(row);

                // If page is full, write it and create a new one
                if (rowId == -1) {
                    // Copy data to buffer manager's page
                    page.setData(workedOnPage.getData());

                    // Mark dirty and unpin
                    bufferManager.markDirty(WORKEDON_FILE, currentPageId);
                    bufferManager.unpinPage(WORKEDON_FILE, currentPageId);

                    // Create a new page
                    page = bufferManager.createPage(WORKEDON_FILE);
                    if (page == null) {
                        throw new RuntimeException("Failed to create new page for WorkedOn");
                    }

                    // Create a new specialized page
                    currentPageId = page.getPid();
                    workedOnPage = new WorkedOnPageImpl(currentPageId);

                    // Try inserting again
                    rowId = workedOnPage.insertWorkedOnRow(row);
                    if (rowId == -1) {
                        throw new RuntimeException("Failed to insert row into new page");
                    }
                }

                rowsLoaded++;

                // Periodically report progress and flush to disk
                if (rowsLoaded % 1000000 == 0) {
                    // Save current page state
                    page.setData(workedOnPage.getData());
                    bufferManager.markDirty(WORKEDON_FILE, currentPageId);

                    // Force write to disk
                    bufferManager.force(WORKEDON_FILE);

                    System.out.println("Loaded " + rowsLoaded + " WorkedOn records, " + directorCount + " directors");
                }
            }

            // Save final page
            page.setData(workedOnPage.getData());
            bufferManager.markDirty(WORKEDON_FILE, currentPageId);
            bufferManager.unpinPage(WORKEDON_FILE, currentPageId);
            bufferManager.force(WORKEDON_FILE);

            System.out.println("Total directors found: " + directorCount);

        } catch (IOException e) {
            System.err.println("Error loading WorkedOn data: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to load WorkedOn data: " + e.getMessage());
        }

        return rowsLoaded;
    }

    /**
     * Loads data from the IMDB name.basics.tsv file into the People table.
     * 
     * @param bufferManager The extended buffer manager
     * @param filepath      The path to the name.basics.tsv file
     * @return The number of rows loaded
     */
    public static int loadPeopleData(ExtendedBufferManager bufferManager, String filepath) {
        int rowsLoaded = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            // Skip header line
            String line = reader.readLine();
            System.out.println("People header: " + line);

            // Create first page
            Page page = bufferManager.createPage(PEOPLE_FILE);
            if (page == null) {
                throw new RuntimeException("Failed to create initial page for People");
            }

            // Create a PeoplePageImpl with the same page ID
            PeoplePageImpl peoplePage = new PeoplePageImpl(page.getPid());

            int currentPageId = page.getPid();

            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\t");
                if (fields.length < 2) {
                    continue;
                }

                String personId = fields[0]; // nconst
                if (personId.getBytes().length > 10) {
                    continue;
                }

                String name = fields[1]; // primaryName
                if (name.getBytes().length > 105) {
                    name = name.substring(0, 105);
                }

                PeopleRow row = new PeopleRow(personId, name);
                int rowId = peoplePage.insertPeopleRow(row);

                if (rowId == -1) {
                    // Page is full, update and create a new page

                    // Copy data back to the buffer manager's page and mark it dirty
                    page.setData(peoplePage.getData());
                    bufferManager.markDirty(PEOPLE_FILE, currentPageId);
                    bufferManager.unpinPage(PEOPLE_FILE, currentPageId);

                    // Create a new page
                    page = bufferManager.createPage(PEOPLE_FILE);
                    if (page == null) {
                        throw new RuntimeException("Failed to create a new page for People");
                    }

                    // Create a new PeoplePageImpl with the same page ID
                    peoplePage = new PeoplePageImpl(page.getPid());
                    currentPageId = page.getPid();

                    // Try inserting again
                    rowId = peoplePage.insertPeopleRow(row);
                    if (rowId == -1) {
                        throw new RuntimeException("Failed to insert row into new page for People");
                    }
                }

                rowsLoaded++;

                if (rowsLoaded % 100000 == 0) {
                    // Periodically flush pages to ensure data is saved
                    page.setData(peoplePage.getData());
                    bufferManager.markDirty(PEOPLE_FILE, currentPageId);
                    bufferManager.force(PEOPLE_FILE);

                    System.out.println("Loaded " + rowsLoaded + " People records");
                }
            }

            // Save the last page
            if (page != null) {
                page.setData(peoplePage.getData());
                bufferManager.markDirty(PEOPLE_FILE, currentPageId);
                bufferManager.unpinPage(PEOPLE_FILE, currentPageId);
                bufferManager.force(PEOPLE_FILE);
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to load People data: " + e.getMessage());
        }

        return rowsLoaded;
    }
}