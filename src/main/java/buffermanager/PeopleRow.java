package buffermanager;

/**
 * Represents a row in the People table with fixed-length fields.
 * Uses byte arrays for primitive data types to enable serialization.
 */
public class PeopleRow {
    // Fixed size fields for the People table
    public byte[] personId; // 10 characters
    public byte[] name; // 105 characters

    /**
     * Creates a new row with the given person ID and name.
     * 
     * @param personId The person ID (10 characters)
     * @param name     The person's name (up to 105 characters)
     */
    public PeopleRow(byte[] personId, byte[] name) {
        this.personId = personId;
        this.name = name;
    }

    /**
     * Creates a new row with the given person ID and name as strings.
     * Converts them to fixed-length byte arrays.
     * 
     * @param personId The person ID as a String
     * @param name     The person's name as a String
     */
    public PeopleRow(String personId, String name) {
        // Convert String to fixed-length byte arrays
        this.personId = new byte[10];
        this.name = new byte[105];

        // Copy personId bytes (pad with spaces if shorter)
        byte[] personIdBytes = personId.getBytes();
        System.arraycopy(personIdBytes, 0, this.personId, 0,
                Math.min(personIdBytes.length, this.personId.length));

        // Copy name bytes (pad with spaces if shorter)
        byte[] nameBytes = name.getBytes();
        System.arraycopy(nameBytes, 0, this.name, 0,
                Math.min(nameBytes.length, this.name.length));
    }

    /**
     * Returns the total size of this row in bytes.
     * 
     * @return The size in bytes
     */
    public int getSize() {
        return personId.length + name.length;
    }

    /**
     * Converts the row to a string representation.
     * 
     * @return A string representation of the row
     */
    @Override
    public String toString() {
        return "PeopleRow{" +
                "personId='" + new String(personId).trim() + '\'' +
                ", name='" + new String(name).trim() + '\'' +
                '}';
    }
}