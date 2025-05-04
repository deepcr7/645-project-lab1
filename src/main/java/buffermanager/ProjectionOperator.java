package buffermanager;

import java.io.File;
import java.io.IOException;

/**
 * Operator that projects (selects) specific columns from tuples.
 */
public class ProjectionOperator implements Operator {
    private final Operator childOperator;
    private final String[] outputColumnNames;
    private final int[] columnIndexes;
    private boolean materialize;
    private String tempTableFile;
    private ExtendedBufferManager bufferManager;
    private MaterializedTable materializedTable;
    private boolean firstNextCall;

    /**
     * Creates a new projection operator with the given child operator and output
     * column names.
     * 
     * @param childOperator     The child operator to get tuples from
     * @param outputColumnNames The names of the columns to project
     * @param inputColumnNames  The names of the columns in the input tuples
     */
    public ProjectionOperator(Operator childOperator, String[] outputColumnNames, String[] inputColumnNames) {
        this.childOperator = childOperator;
        this.outputColumnNames = outputColumnNames;
        this.columnIndexes = new int[outputColumnNames.length];
        this.materialize = false;
        this.firstNextCall = true;

        // Map output column names to input column indexes
        for (int i = 0; i < outputColumnNames.length; i++) {
            boolean found = false;
            for (int j = 0; j < inputColumnNames.length; j++) {
                if (outputColumnNames[i].equals(inputColumnNames[j])) {
                    columnIndexes[i] = j;
                    found = true;
                    break;
                }
            }

            if (!found) {
                throw new IllegalArgumentException("Column not found: " + outputColumnNames[i]);
            }
        }
    }

    /**
     * Configures this projection operator to materialize its output into a
     * temporary table.
     * 
     * @param bufferManager The buffer manager to use for creating the temporary
     *                      table
     * @param tempTableFile The name of the temporary table file
     */
    public void setMaterialize(ExtendedBufferManager bufferManager, String tempTableFile) {
        this.materialize = true;
        this.bufferManager = bufferManager;
        this.tempTableFile = tempTableFile;
    }

    @Override
    public void open() {
        // Open the child operator
        childOperator.open();
        firstNextCall = true;
    }

    @Override
    public Tuple next() {
        if (materialize) {
            // For materialized projection, we create the table on the first call to next()
            if (firstNextCall) {
                materializeOutput();
                firstNextCall = false;
            }

            // Then we read from the materialized table
            return materializedTable.next();
        } else {
            // For on-the-fly projection, we get a tuple from the child and project it
            Tuple inputTuple = childOperator.next();

            if (inputTuple == null) {
                return null;
            }

            return projectTuple(inputTuple);
        }
    }

    @Override
    public void close() {
        // Close the child operator
        childOperator.close();

        // If we materialized, close the materialized table but DO NOT delete the file
        if (materialize && materializedTable != null) {
            materializedTable.close();
            // Don't delete the file here - it needs to be available for other operators
            // The file will be deleted by the QueryExecutor when the query is complete
        }
    }

    /**
     * Projects the given tuple to include only the selected columns.
     * 
     * @param inputTuple The input tuple to project
     * @return The projected tuple
     */
    private Tuple projectTuple(Tuple inputTuple) {
        String[] inputValues = inputTuple.getValues();
        String[] outputValues = new String[columnIndexes.length];

        for (int i = 0; i < columnIndexes.length; i++) {
            outputValues[i] = inputValues[columnIndexes[i]];
        }

        return new Tuple(outputValues, outputColumnNames);
    }

    private void materializeOutput() {
        System.err.println("Starting materialization...");
        System.err.println("Temporary file: " + tempTableFile);

        // Use absolute path for the temporary file
        File file = new File(tempTableFile);
        if (!file.isAbsolute()) {
            file = file.getAbsoluteFile();
            tempTableFile = file.getAbsolutePath();
            System.err.println("Using absolute path: " + tempTableFile);
        }

        // Ensure directory exists for temporary file
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            boolean created = parent.mkdirs();
            System.err.println("Created directory: " + parent.getAbsolutePath() + " - " + created);
        }

        // Create a new materialized table
        materializedTable = new MaterializedTable(bufferManager, tempTableFile, outputColumnNames);

        // Open the materialized table
        try {
            materializedTable.open();
            System.err.println("Materialized table opened successfully");
        } catch (Exception e) {
            System.err.println("Error opening materialized table: " + e.getMessage());
            throw new RuntimeException("Failed to open materialized table", e);
        }

        // Read all tuples from the child operator, project them, and insert them into
        // the materialized table
        Tuple inputTuple;
        int tupleCount = 0;
        while ((inputTuple = childOperator.next()) != null) {
            Tuple projectedTuple = projectTuple(inputTuple);
            materializedTable.insert(projectedTuple);
            tupleCount++;
            if (tupleCount % 1000 == 0) {
                System.err.println("Materialized " + tupleCount + " tuples...");
            }
        }

        // Force all pages to disk
        bufferManager.force(tempTableFile);
        System.err.println("Forced buffer to disk");

        // Verify file exists
        file = new File(tempTableFile);
        if (!file.exists()) {
            System.err.println("ERROR: Temporary file not created: " + tempTableFile);
            // Create an empty file to proceed with the query
            try {
                boolean created = file.createNewFile();
                System.err.println("Created empty file: " + created);
                if (created) {
                    // Reset the materialized table
                    materializedTable.reset();
                    System.err.println("Reset complete after creating empty file");
                    return;
                }
            } catch (IOException e) {
                System.err.println("Failed to create empty file: " + e.getMessage());
            }
            throw new RuntimeException("Temporary file not created: " + tempTableFile);
        }

        System.err.println("File size before reset: " + file.length() + " bytes");

        // Reset the materialized table for reading
        try {
            materializedTable.reset();
            System.err.println("Reset complete - ready to read materialized table");
        } catch (Exception e) {
            System.err.println("Error resetting materialized table: " + e.getMessage());
            throw new RuntimeException("Failed to reset materialized table", e);
        }
    }

    /**
     * Helper class for materialized tables.
     */
    private static class MaterializedTable {
        private final ExtendedBufferManager bufferManager;
        private final String filename;
        private final String[] columnNames;
        private int currentPageId;
        private int currentRowId;
        private Page currentPage;
        private int totalPages;
        private boolean isOpen;

        /**
         * Creates a new materialized table with the given buffer manager, filename, and
         * column names.
         * 
         * @param bufferManager The buffer manager to use
         * @param filename      The name of the file to store the table in
         * @param columnNames   The names of the columns in the table
         */
        public MaterializedTable(ExtendedBufferManager bufferManager, String filename, String[] columnNames) {
            this.bufferManager = bufferManager;
            this.filename = filename;
            this.columnNames = columnNames;
            this.currentPageId = -1;
            this.currentRowId = -1;
            this.currentPage = null;
            this.totalPages = 0;
            this.isOpen = false;

            // Ensure the parent directory exists
            File file = new File(filename);
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
        }

        public void open() {
            if (isOpen) {
                // Already open, nothing to do
                System.err.println("Table already open, not reopening");
                return;
            }

            // Delete the file if it exists to start fresh
            File file = new File(filename);
            if (file.exists()) {
                System.err.println("Open: Deleting existing file: " + filename);
                if (!file.delete()) {
                    System.err.println("Warning: Failed to delete existing file: " + filename);
                    // Continue anyway, we'll try to create a new page
                }
            }

            // Make sure the parent directory exists
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                boolean created = parent.mkdirs();
                System.err.println("Created parent directory: " + created);
            }

            // Create the first page
            try {
                currentPage = bufferManager.createPage(filename);
                if (currentPage == null) {
                    throw new RuntimeException("Failed to create page for materialized table: " + filename);
                }

                currentPageId = currentPage.getPid();
                currentRowId = 0;
                totalPages = 1;
                isOpen = true;

                System.err.println("Open: Created first page with ID: " + currentPageId);

                // Verify the file was created
                if (!file.exists()) {
                    System.err.println("Warning: File not created after bufferManager.createPage");
                    // Try to touch the file
                    try {
                        boolean touched = file.createNewFile();
                        System.err.println("Touched file: " + touched);
                    } catch (IOException e) {
                        System.err.println("Failed to touch file: " + e.getMessage());
                    }
                }
            } catch (Exception e) {
                System.err.println("Error creating page: " + e.getMessage());
                throw new RuntimeException("Failed to create page for materialized table", e);
            }
        }

        /**
         * Inserts a tuple into the materialized table.
         * 
         * @param tuple The tuple to insert
         */
        public void insert(Tuple tuple) {
            if (!isOpen) {
                throw new IllegalStateException("Materialized table is not open");
            }

            // Create a row from the tuple
            Row row = createRowFromTuple(tuple);

            // Insert the row into the current page
            int rowId = currentPage.insertRow(row);

            // If the page is full, create a new page
            if (rowId == -1) {
                // Mark the current page as dirty and unpin it
                bufferManager.markDirty(filename, currentPageId);
                bufferManager.unpinPage(filename, currentPageId);

                // Create a new page
                try {
                    currentPage = bufferManager.createPage(filename);
                    if (currentPage == null) {
                        throw new RuntimeException("Failed to create page for materialized table");
                    }

                    currentPageId = currentPage.getPid();
                    totalPages++;

                    // Try inserting again
                    rowId = currentPage.insertRow(row);
                    if (rowId == -1) {
                        throw new RuntimeException("Failed to insert row into new page");
                    }
                } catch (Exception e) {
                    System.err.println("Error creating new page: " + e.getMessage());
                    throw new RuntimeException("Failed to create new page", e);
                }
            }

            currentRowId = rowId;
        }

        /**
         * Resets the materialized table for reading.
         */
        public void reset() {
            // If not open, open it
            if (!isOpen) {
                open();
                return;
            }

            // Make sure the current page is properly handled
            if (currentPage != null) {
                // Flush any pending changes
                bufferManager.markDirty(filename, currentPageId);
                bufferManager.unpinPage(filename, currentPageId);
                bufferManager.force(filename);
                currentPage = null;
            }

            // Reset to the first page and row
            currentPageId = 0;
            currentRowId = 0;

            // Check if the file exists
            File file = new File(filename);
            System.err.println("Reset: File exists? " + file.exists());
            if (file.exists()) {
                System.err.println("Reset: File size: " + file.length() + " bytes");
            } else {
                throw new RuntimeException("Temporary file does not exist: " + filename);
            }

            // Load the first page
            try {
                currentPage = bufferManager.getPage(filename, currentPageId);
                if (currentPage == null) {
                    throw new RuntimeException("Failed to load first page of materialized table: " + filename);
                }
                System.err.println("Reset: Successfully loaded first page");
            } catch (Exception e) {
                System.err.println("Error loading first page: " + e.getMessage());
                throw new RuntimeException("Failed to load first page of materialized table: " + filename, e);
            }
        }

        public Tuple next() {
            if (!isOpen) {
                throw new IllegalStateException("Materialized table is not open");
            }

            // Get the row from the current page
            Row row = currentPage.getRow(currentRowId);

            // If we've reached the end of the page, move to the next page
            if (row == null) {
                // If this is the last page, we're done
                if (currentPageId >= totalPages - 1) {
                    return null;
                }

                // Unpin the current page
                bufferManager.unpinPage(filename, currentPageId);

                // Move to the next page
                currentPageId++;
                currentRowId = 0;

                try {
                    // Load the next page
                    currentPage = bufferManager.getPage(filename, currentPageId);
                    if (currentPage == null) {
                        System.err.println("Warning: Failed to load page " + currentPageId + " of materialized table");
                        return null; // Just return null instead of throwing an exception
                    }
                } catch (Exception e) {
                    System.err.println("Error loading next page: " + e.getMessage());
                    return null; // Just return null instead of throwing an exception
                }

                // Get the row from the new page
                row = currentPage.getRow(currentRowId);
                if (row == null) {
                    return null;
                }
            }

            // Create a tuple from the row
            Tuple tuple = createTupleFromRow(row);

            // Move to the next row
            currentRowId++;

            return tuple;
        }

        /**
         * Closes the materialized table.
         */
        public void close() {
            if (isOpen && currentPage != null) {
                // Unpin the current page
                bufferManager.unpinPage(filename, currentPageId);
                currentPage = null;
            }

            isOpen = false;
        }

        /**
         * Creates a row from a tuple for insertion into the materialized table.
         * 
         * @param tuple The tuple to convert to a row
         * @return The row created from the tuple
         */
        private Row createRowFromTuple(Tuple tuple) {
            StringBuilder sb = new StringBuilder();

            // Concatenate all values with fixed width
            String[] values = tuple.getValues();
            for (String value : values) {
                // Pad or truncate each value to 30 characters
                String paddedValue = value;
                if (paddedValue.length() > 30) {
                    paddedValue = paddedValue.substring(0, 30);
                }
                sb.append(paddedValue);
            }

            // Create a row with the concatenated values
            String movieId = "mat" + String.format("%06d", currentRowId);
            return new Row(movieId, sb.toString());
        }

        /**
         * Creates a tuple from a row read from the materialized table.
         * 
         * @param row The row to convert to a tuple
         * @return The tuple created from the row
         */
        private Tuple createTupleFromRow(Row row) {
            // Extract the concatenated values from the row
            String concatenatedValues = new String(row.title).trim();

            // Split into individual values based on fixed width
            String[] values = new String[columnNames.length];
            for (int i = 0; i < columnNames.length; i++) {
                int start = i * 30;
                int end = Math.min(start + 30, concatenatedValues.length());

                if (start < concatenatedValues.length()) {
                    values[i] = concatenatedValues.substring(start, end).trim();
                } else {
                    values[i] = "";
                }
            }

            return new Tuple(values, columnNames);
        }
    }
}