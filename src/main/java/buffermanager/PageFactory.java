package buffermanager;

/**
 * Factory class for creating different types of pages based on table type.
 */
public class PageFactory {

    public enum TableType {
        MOVIES,
        WORKEDON,
        PEOPLE
    }

    /**
     * Creates a new empty page for the specified table type.
     * 
     * @param tableType The type of table the page is for
     * @param pageId    The ID to assign to the new page
     * @return A new page of the appropriate type
     */
    public static Page createPage(TableType tableType, int pageId) {
        switch (tableType) {
            case MOVIES:
                return new PageImpl(pageId);
            case WORKEDON:
                return new WorkedOnPageImpl(pageId);
            case PEOPLE:
                return new PeoplePageImpl(pageId);
            default:
                throw new IllegalArgumentException("Unknown table type: " + tableType);
        }
    }

    /**
     * Creates a page from existing data for the specified table type.
     * 
     * @param tableType The type of table the page is for
     * @param pageId    The ID to assign to the page
     * @param data      The raw page data
     * @return A page of the appropriate type initialized with the provided data
     */
    public static Page createPageFromData(TableType tableType, int pageId, byte[] data) {
        switch (tableType) {
            case MOVIES:
                return new PageImpl(pageId, data);
            case WORKEDON:
                return new WorkedOnPageImpl(pageId, data);
            case PEOPLE:
                return new PeoplePageImpl(pageId, data);
            default:
                throw new IllegalArgumentException("Unknown table type: " + tableType);
        }
    }
}