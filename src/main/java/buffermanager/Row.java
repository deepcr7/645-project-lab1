package buffermanager;

/**
 * Represents a row in the Movies table with fixed-length fields.
 * Uses byte arrays for primitive data types to enable serialization.
 */
public class Row {
    // Fixed size fields for the Movies table
    public byte[] movieId;  // 9 characters
    public byte[] title;    // 30 characters
    
    /**
     * Creates a new row with the given movie ID and title.
     * 
     * @param movieId The movie ID (9 characters)
     * @param title The movie title (up to 30 characters)
     */
    public Row(byte[] movieId, byte[] title) {
        this.movieId = movieId;
        this.title = title;
    }
    
    /**
     * Creates a new row with the given movie ID and title as strings.
     * Converts them to fixed-length byte arrays.
     * 
     * @param movieId The movie ID as a String
     * @param title The movie title as a String
     */
    public Row(String movieId, String title) {
        // Convert String to fixed-length byte arrays
        this.movieId = new byte[9];
        this.title = new byte[30];
        
        // Copy movieId bytes (pad with spaces if shorter)
        byte[] movieIdBytes = movieId.getBytes();
        System.arraycopy(movieIdBytes, 0, this.movieId, 0, 
                Math.min(movieIdBytes.length, this.movieId.length));
        
        // Copy title bytes (pad with spaces if shorter)
        byte[] titleBytes = title.getBytes();
        System.arraycopy(titleBytes, 0, this.title, 0, 
                Math.min(titleBytes.length, this.title.length));
    }
    
    /**
     * Returns the total size of this row in bytes.
     * 
     * @return The size in bytes
     */
    public int getSize() {
        return movieId.length + title.length;
    }
    
    /**
     * Converts the row to a string representation.
     * 
     * @return A string representation of the row
     */
    @Override
    public String toString() {
        return "Row{" +
                "movieId='" + new String(movieId).trim() + '\'' +
                ", title='" + new String(title).trim() + '\'' +
                '}';
    }
}