package buffermanager;

import java.io.File;

/**
 * Operator that scans a table sequentially.
 */
public class ScanOperator implements Operator {
    private final ExtendedBufferManager bufferManager;
    private final String filename;
    private final PageFactory.TableType tableType;
    private final String[] columnNames;

    private int currentPageId;
    private int currentRowId;
    private Page currentPage;
    private int totalPages;

    /**
     * Creates a new scan operator for the specified table.
     * 
     * @param bufferManager The buffer manager
     * @param filename      The name of the table file
     * @param tableType     The type of table being scanned
     * @param columnNames   The names of the columns in the table
     */
    public ScanOperator(ExtendedBufferManager bufferManager, String filename,
            PageFactory.TableType tableType, String[] columnNames) {
        this.bufferManager = bufferManager;
        this.filename = filename;
        this.tableType = tableType;
        this.columnNames = columnNames;
        this.currentPageId = -1;
        this.currentRowId = -1;
        this.currentPage = null;

        // Calculate total pages in the file
        File file = new File(filename);
        this.totalPages = (int) (file.length() / PageImpl.PAGE_SIZE);
    }

    @Override
    public void open() {
        // Move to the first page and row
        currentPageId = 0;
        currentRowId = -1;

        // Load the first page
        loadNextPage();
    }

    @Override
    public Tuple next() {
        // Check if we need to move to the next row
        currentRowId++;

        // If we've gone past the end of the current page, load the next page
        if (currentPage != null) {
            Object row = getRowFromPage(currentPage, currentRowId);
            if (row == null) {
                if (currentPageId < totalPages - 1) {
                    // Unpin the current page
                    bufferManager.unpinPage(filename, currentPageId);
                    currentPage = null;

                    // Move to the next page
                    currentPageId++;
                    currentRowId = 0;

                    // Load the next page
                    loadNextPage();

                    // Try to get the first row from the new page
                    row = getRowFromPage(currentPage, currentRowId);
                }
            }

            // If we found a row, convert it to a tuple and return it
            if (row != null) {
                return createTupleFromRow(row);
            }
        }

        // If we get here, there are no more rows to return
        return null;
    }

    @Override
    public void close() {
        // Unpin the current page if there is one
        if (currentPage != null) {
            bufferManager.unpinPage(filename, currentPageId);
            currentPage = null;
        }

        // Reset state
        currentPageId = -1;
        currentRowId = -1;
    }

    /**
     * Loads the next page from the table.
     */
    private void loadNextPage() {
        try {
            // Get the page from the buffer manager
            Page page = bufferManager.getPage(filename, currentPageId);
            if (page == null) {
                currentPage = null;
                return;
            }

            // Get the raw data from the page
            byte[] pageData = page.getData();

            // Create the appropriate page type based on the table type
            switch (tableType) {
                case MOVIES:
                    // For Movies, we can use the page directly
                    currentPage = page;
                    break;
                case WORKEDON:
                    // For WorkedOn, create a specialized page
                    WorkedOnPageImpl workedOnPage = new WorkedOnPageImpl(currentPageId);
                    workedOnPage.setData(pageData);
                    bufferManager.unpinPage(filename, currentPageId);
                    currentPage = workedOnPage;
                    break;
                case PEOPLE:
                    // For People, create a specialized page
                    PeoplePageImpl peoplePage = new PeoplePageImpl(currentPageId);
                    peoplePage.setData(pageData);
                    bufferManager.unpinPage(filename, currentPageId);
                    currentPage = peoplePage;
                    break;
                default:
                    currentPage = null;
                    bufferManager.unpinPage(filename, currentPageId);
            }
        } catch (Exception e) {
            System.err.println("Error loading page " + currentPageId + " from " + filename + ": " + e.getMessage());
            e.printStackTrace();
            currentPage = null;
        }
    }

    /**
     * Gets a row from the current page based on the table type.
     * 
     * @param page  The page to get the row from
     * @param rowId The ID of the row to get
     * @return The row object, or null if the row doesn't exist
     */
    private Object getRowFromPage(Page page, int rowId) {
        try {
            switch (tableType) {
                case MOVIES:
                    return page.getRow(rowId);
                case WORKEDON:
                    if (page instanceof WorkedOnPageImpl) {
                        return ((WorkedOnPageImpl) page).getWorkedOnRow(rowId);
                    }
                    return null;
                case PEOPLE:
                    if (page instanceof PeoplePageImpl) {
                        return ((PeoplePageImpl) page).getPeopleRow(rowId);
                    }
                    return null;
                default:
                    return null;
            }
        } catch (Exception e) {
            System.err.println("Error getting row from page: " + e.getMessage());
            return null;
        }
    }

    private Tuple createTupleFromRow(Object row) {
        String[] values;

        try {
            switch (tableType) {
                case MOVIES:
                    Row movieRow = (Row) row;
                    values = new String[] {
                            new String(movieRow.movieId).trim(),
                            new String(movieRow.title).trim()
                    };
                    break;
                case WORKEDON:
                    WorkedOnRow workedOnRow = (WorkedOnRow) row;
                    values = new String[] {
                            new String(workedOnRow.movieId).trim(),
                            new String(workedOnRow.personId).trim(),
                            new String(workedOnRow.category).trim()
                    };
                    break;
                case PEOPLE:
                    PeopleRow peopleRow = (PeopleRow) row;
                    values = new String[] {
                            new String(peopleRow.personId).trim(),
                            new String(peopleRow.name).trim()
                    };
                    break;
                default:
                    throw new IllegalStateException("Unknown table type: " + tableType);
            }
        } catch (Exception e) {
            System.err.println("Error creating tuple from row: " + e.getMessage());
            return null;
        }

        return new Tuple(values, columnNames);
    }
}