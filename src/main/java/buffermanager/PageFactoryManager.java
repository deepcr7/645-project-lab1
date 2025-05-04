package buffermanager;

public class PageFactoryManager {

    /**
     * Creates a page for the specified table type.
     * 
     * @param bufferManager The buffer manager
     * @param filename      The file to create the page in
     * @param tableType     The type of table
     * @return The created page
     */
    public static Page createPage(ExtendedBufferManager bufferManager, String filename,
            PageFactory.TableType tableType) {
        Page page = bufferManager.createPage(filename);
        if (page == null) {
            return null;
        }

        // Create a page of the appropriate type
        int pageId = page.getPid();
        bufferManager.unpinPage(filename, pageId);

        switch (tableType) {
            case MOVIES:
                return page; // PageImpl is already correct for Movies
            case WORKEDON:
                return new WorkedOnPageImpl(pageId);
            case PEOPLE:
                return new PeoplePageImpl(pageId);
            default:
                throw new IllegalArgumentException("Unknown table type: " + tableType);
        }
    }

    /**
     * Gets a page for the specified table type.
     * 
     * @param bufferManager The buffer manager
     * @param filename      The file to get the page from
     * @param pageId        The ID of the page to get
     * @param tableType     The type of table
     * @return The retrieved page
     */
    public static Page getPage(ExtendedBufferManager bufferManager, String filename, int pageId,
            PageFactory.TableType tableType) {
        Page page = bufferManager.getPage(filename, pageId);
        if (page == null) {
            return null;
        }

        switch (tableType) {
            case MOVIES:
                return page; // PageImpl is already correct for Movies
            case WORKEDON:
                WorkedOnPageImpl workedOnPage = new WorkedOnPageImpl(pageId);
                workedOnPage.setData(page.getData());
                bufferManager.unpinPage(filename, pageId);
                return workedOnPage;
            case PEOPLE:
                PeoplePageImpl peoplePage = new PeoplePageImpl(pageId);
                peoplePage.setData(page.getData());
                bufferManager.unpinPage(filename, pageId);
                return peoplePage;
            default:
                throw new IllegalArgumentException("Unknown table type: " + tableType);
        }
    }
}