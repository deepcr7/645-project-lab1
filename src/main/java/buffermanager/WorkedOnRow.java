package buffermanager;

/**
 * Represents a row in the WorkedOn table with fixed-length fields.
 * Uses byte arrays for primitive data types to enable serialization.
 */
public class WorkedOnRow {
    // Fixed size fields for the WorkedOn table
    public byte[] movieId; // 9 characters
    public byte[] personId; // 10 characters
    public byte[] category; // 20 characters

    /**
     * Creates a new row with the given movie ID, person ID, and category.
     * 
     * @param movieId  The movie ID (9 characters)
     * @param personId The person ID (10 characters)
     * @param category The category (20 characters)
     */
    public WorkedOnRow(byte[] movieId, byte[] personId, byte[] category) {
        this.movieId = movieId;
        this.personId = personId;
        this.category = category;
    }

    /**
     * Creates a new row with the given movie ID, person ID, and category as
     * strings.
     * Converts them to fixed-length byte arrays.
     * 
     * @param movieId  The movie ID as a String
     * @param personId The person ID as a String
     * @param category The category as a String
     */
    public WorkedOnRow(String movieId, String personId, String category) {
        // Convert String to fixed-length byte arrays
        this.movieId = new byte[9];
        this.personId = new byte[10];
        this.category = new byte[20];

        // Copy movieId bytes (pad with spaces if shorter)
        byte[] movieIdBytes = movieId.getBytes();
        System.arraycopy(movieIdBytes, 0, this.movieId, 0,
                Math.min(movieIdBytes.length, this.movieId.length));

        // Copy personId bytes (pad with spaces if shorter)
        byte[] personIdBytes = personId.getBytes();
        System.arraycopy(personIdBytes, 0, this.personId, 0,
                Math.min(personIdBytes.length, this.personId.length));

        // Copy category bytes (pad with spaces if shorter)
        byte[] categoryBytes = category.getBytes();
        System.arraycopy(categoryBytes, 0, this.category, 0,
                Math.min(categoryBytes.length, this.category.length));
    }

    /**
     * Returns the total size of this row in bytes.
     * 
     * @return The size in bytes
     */
    public int getSize() {
        return movieId.length + personId.length + category.length;
    }

    /**
     * Converts the row to a string representation.
     * 
     * @return A string representation of the row
     */
    @Override
    public String toString() {
        return "WorkedOnRow{" +
                "movieId='" + new String(movieId).trim() + '\'' +
                ", personId='" + new String(personId).trim() + '\'' +
                ", category='" + new String(category).trim() + '\'' +
                '}';
    }
}