package buffermanager;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

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
    private boolean isMaterialized;
    private int materializedTupleCount;

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
        this.isMaterialized = false;
        this.materializedTupleCount = 0;

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
    // Modify ProjectionOperator.java - Add optimized materialization for WorkedOn
    // Add this method to your ProjectionOperator class

    private void materializeOutputOptimized() {
        System.out.println("Starting optimized materialization to file: " + tempTableFile);

        try {
            // Delete existing file if it exists
            File file = new File(tempTableFile);
            if (file.exists()) {
                file.delete();
            }

            // Ensure directory exists
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }

            // Create a new materialized table
            materializedTable = new MaterializedTable(bufferManager, tempTableFile, outputColumnNames);
            materializedTable.open();
            System.out.println("Materialized table opened successfully");

            // Read all tuples from the child operator, project them, and insert them
            // into the materialized table
            Tuple inputTuple;
            int tupleCount = 0;
            long startTime = System.currentTimeMillis();
            int reportInterval = 1000000; // Log every million rows

            while ((inputTuple = childOperator.next()) != null) {
                Tuple projectedTuple = projectTuple(inputTuple);
                materializedTable.insert(projectedTuple);
                tupleCount++;

                if (tupleCount % reportInterval == 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    double rate = tupleCount / (elapsed / 1000.0);
                    System.out.println("Materialized " + tupleCount + " tuples... (" +
                            String.format("%.2f", rate) + " tuples/sec)");
                }
            }
            materializedTupleCount = tupleCount;
            long elapsed = System.currentTimeMillis() - startTime;
            double rate = tupleCount / (elapsed / 1000.0);
            System.out.println("Materialized " + tupleCount + " tuples total in " +
                    elapsed + "ms (" + String.format("%.2f", rate) + " tuples/sec)");

            // Force all pages to disk
            bufferManager.force(tempTableFile);

            // Reset the materialized table for reading
            materializedTable.reset();
            System.out.println("Reset complete - ready to read from materialized table");

        } catch (Exception e) {
            System.err.println("Error during materialization: " + e.getMessage());
            e.printStackTrace();
            materializedTable = null;
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

        // Create parent directory if it doesn't exist
        File file = new File(tempTableFile);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }
    }

    @Override
    public void open() {
        // Open the child operator
        childOperator.open();
        firstNextCall = true;
        isMaterialized = false;
        materializedTupleCount = 0;
    }

    @Override
    public Tuple next() {
        if (materialize) {
            // For materialized projection, we create the table on the first call to next()
            if (firstNextCall) {
                materializeOutput();
                firstNextCall = false;
                isMaterialized = true;
            }

            // Then we read from the materialized table
            if (materializedTable != null) {
                return materializedTable.next();
            } else {
                return null;
            }
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
            if (columnIndexes[i] < inputValues.length) {
                outputValues[i] = inputValues[columnIndexes[i]];
            } else {
                outputValues[i] = ""; // Default empty value for non-existent columns
            }
        }

        return new Tuple(outputValues, outputColumnNames);
    }

    private void materializeOutput() {

        try {
            // Delete existing file if it exists
            File file = new File(tempTableFile);
            if (file.exists()) {
                file.delete();
            }

            // Ensure directory exists
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }

            // Create a new materialized table
            materializedTable = new MaterializedTable(bufferManager, tempTableFile, outputColumnNames);

            // Open the materialized table
            materializedTable.open();
            System.out.println("Materialized table opened successfully");

            // Read all tuples from the child operator, project them, and insert them into
            // the materialized table
            Tuple inputTuple;
            int tupleCount = 0;
            while ((inputTuple = childOperator.next()) != null) {
                Tuple projectedTuple = projectTuple(inputTuple);
                materializedTable.insert(projectedTuple);
                tupleCount++;
            }
            materializedTupleCount = tupleCount;
            System.out.println("Materialized " + tupleCount + " tuples total");

            // Force all pages to disk
            bufferManager.force(tempTableFile);

            // Reset the materialized table for reading
            materializedTable.reset();

        } catch (Exception e) {
            System.err.println("Error during materialization: " + e.getMessage());
            e.printStackTrace();
            materializedTable = null;
        }
    }

    /**
     * Helper class for materialized tables.
     */
    private class MaterializedTable {
        private final ExtendedBufferManager bufferManager;
        private final String filename;
        private final String[] columnNames;
        private final int[] columnSizes; // Fixed size for each column
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

            // Define fixed sizes for each column (customize as needed)
            this.columnSizes = new int[columnNames.length];
            for (int i = 0; i < columnNames.length; i++) {
                if (columnNames[i].toLowerCase().contains("movieid")) {
                    columnSizes[i] = 9;
                } else if (columnNames[i].toLowerCase().contains("personid")) {
                    columnSizes[i] = 10;
                } else if (columnNames[i].toLowerCase().contains("category")) {
                    columnSizes[i] = 20;
                } else if (columnNames[i].toLowerCase().contains("name")) {
                    columnSizes[i] = 105;
                } else if (columnNames[i].toLowerCase().contains("title")) {
                    columnSizes[i] = 30;
                } else {
                    columnSizes[i] = 30; // Default
                }
            }
        }

        public void open() {

            if (isOpen)
                return;
            File file = new File(filename);
            if (file.exists())
                file.delete();
            File parent = file.getParentFile();
            if (parent != null && !parent.exists())
                parent.mkdirs();
            currentPage = bufferManager.createPage(filename);
            if (currentPage == null)
                throw new RuntimeException("Failed to create page for materialized table: " + filename);
            currentPageId = currentPage.getPid();
            currentRowId = 0;
            totalPages = 1;
            isOpen = true;
        }

        /**
         * Inserts a tuple into the materialized table.
         * 
         * @param tuple The tuple to insert
         */
        public void insert(Tuple tuple) {
            if (!isOpen)
                throw new IllegalStateException("Materialized table is not open");
            Row row = createRowFromTuple(tuple);
            int rowId = currentPage.insertRow(row);
            if (rowId == -1) {
                bufferManager.markDirty(filename, currentPageId);
                bufferManager.unpinPage(filename, currentPageId);
                currentPage = bufferManager.createPage(filename);
                if (currentPage == null)
                    throw new RuntimeException("Failed to create page for materialized table");
                currentPageId = currentPage.getPid();
                totalPages++;
                rowId = currentPage.insertRow(row);
                if (rowId == -1)
                    throw new RuntimeException("Failed to insert row into new page");
            }
            currentRowId = rowId;
        }

        /**
         * Resets the materialized table for reading.
         */
        public void reset() {
            if (!isOpen)
                open();
            if (currentPage != null) {
                bufferManager.markDirty(filename, currentPageId);
                bufferManager.unpinPage(filename, currentPageId);
                currentPage = null;
            }
            currentPageId = 0;
            currentRowId = 0;
            File file = new File(filename);
            if (!file.exists())
                throw new RuntimeException("Temporary file does not exist: " + filename);
            currentPage = bufferManager.getPage(filename, currentPageId);
            if (currentPage == null)
                throw new RuntimeException("Failed to load first page of materialized table: " + filename);
        }

        public Tuple next() {
            if (!isOpen)
                throw new IllegalStateException("Materialized table is not open");
            Row row = currentPage.getRow(currentRowId);
            if (row == null) {
                if (currentPageId >= totalPages - 1)
                    return null;
                bufferManager.unpinPage(filename, currentPageId);
                currentPageId++;
                currentRowId = 0;
                currentPage = bufferManager.getPage(filename, currentPageId);
                if (currentPage == null)
                    return null;
                row = currentPage.getRow(currentRowId);
                if (row == null)
                    return null;
            }
            Tuple tuple = createTupleFromRow(row);
            currentRowId++;
            return tuple;
        }

        /**
         * Closes the materialized table.
         */
        public void close() {
            if (isOpen && currentPage != null) {
                bufferManager.unpinPage(filename, currentPageId);
                currentPage = null;
            }
            isOpen = false;
        }

        // --- Optimized: Only write the projected columns, using correct fixed lengths
        // ---
        private Row createRowFromTuple(Tuple tuple) {
            String[] values = tuple.getValues();
            byte[] movieId = new byte[9];
            byte[] rest = new byte[getTotalRestLength()];
            int restOffset = 0;
            for (int i = 0; i < columnNames.length; i++) {
                byte[] valBytes = values[i].getBytes();
                int len = columnSizes[i];
                if (columnNames[i].toLowerCase().contains("movieid")) {
                    System.arraycopy(valBytes, 0, movieId, 0, Math.min(valBytes.length, 9));
                } else {
                    System.arraycopy(valBytes, 0, rest, restOffset, Math.min(valBytes.length, len));
                    restOffset += len;
                }
            }
            return new Row(movieId, rest);
        }

        private Tuple createTupleFromRow(Row row) {
            String[] values = new String[columnNames.length];
            int restOffset = 0;
            for (int i = 0; i < columnNames.length; i++) {
                int len = columnSizes[i];
                if (columnNames[i].toLowerCase().contains("movieid")) {
                    values[i] = new String(row.movieId).trim();
                } else {
                    values[i] = new String(row.title, restOffset, len).trim();
                    restOffset += len;
                }
            }
            return new Tuple(values, columnNames);
        }

        private int getTotalRestLength() {
            int total = 0;
            for (int i = 0; i < columnNames.length; i++) {
                if (!columnNames[i].toLowerCase().contains("movieid")) {
                    total += columnSizes[i];
                }
            }
            return total;
        }
    }
}