package buffermanager;

import java.util.Arrays;

/**
 * Class representing a tuple (row) in the query execution pipeline.
 * A tuple can contain values from any table or combination of tables.
 */
public class Tuple {
    // The column values in the tuple
    private final String[] values;
    // The column names in the tuple
    private final String[] columnNames;

    /**
     * Creates a new tuple with the given values and column names.
     * 
     * @param values      The values for each column
     * @param columnNames The names of the columns
     */
    public Tuple(String[] values, String[] columnNames) {
        if (values.length != columnNames.length) {
            throw new IllegalArgumentException("Values and column names must have the same length");
        }
        this.values = Arrays.copyOf(values, values.length);
        this.columnNames = Arrays.copyOf(columnNames, columnNames.length);
    }

    /**
     * Creates a new tuple by joining two existing tuples.
     * 
     * @param left  The left tuple
     * @param right The right tuple
     */
    public Tuple(Tuple left, Tuple right) {
        int leftLength = left.values.length;
        int rightLength = right.values.length;

        this.values = new String[leftLength + rightLength];
        this.columnNames = new String[leftLength + rightLength];

        System.arraycopy(left.values, 0, this.values, 0, leftLength);
        System.arraycopy(right.values, 0, this.values, leftLength, rightLength);

        System.arraycopy(left.columnNames, 0, this.columnNames, 0, leftLength);
        System.arraycopy(right.columnNames, 0, this.columnNames, leftLength, rightLength);
    }

    /**
     * Gets the value at the specified index.
     * 
     * @param index The index of the value to get
     * @return The value at the specified index
     */
    public String getValue(int index) {
        return values[index];
    }

    /**
     * Gets the value with the specified column name.
     * 
     * @param columnName The name of the column
     * @return The value for the specified column, or null if the column doesn't
     *         exist
     */
    public String getValue(String columnName) {
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].equals(columnName)) {
                return values[i];
            }
        }
        return null;
    }

    /**
     * Gets the number of values in this tuple.
     * 
     * @return The number of values
     */
    public int getSize() {
        return values.length;
    }

    /**
     * Gets all values in this tuple.
     * 
     * @return The values in this tuple
     */
    public String[] getValues() {
        return Arrays.copyOf(values, values.length);
    }

    /**
     * Gets all column names in this tuple.
     * 
     * @return The column names in this tuple
     */
    public String[] getColumnNames() {
        return Arrays.copyOf(columnNames, columnNames.length);
    }

    /**
     * Returns a string representation of this tuple in CSV format.
     * 
     * @return A string representation of this tuple
     */
    public String toCsv() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            sb.append(values[i]);
            if (i < values.length - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Tuple{");
        for (int i = 0; i < values.length; i++) {
            sb.append(columnNames[i]).append("='").append(values[i]).append("'");
            if (i < values.length - 1) {
                sb.append(", ");
            }
        }
        sb.append("}");
        return sb.toString();
    }
}