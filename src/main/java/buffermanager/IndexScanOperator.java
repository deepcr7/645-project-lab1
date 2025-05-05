package buffermanager;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;

/**
 * Operator that scans a table using a B+ tree index for a range query.
 */
public class IndexScanOperator implements Operator {
    private final TreeMap<String, List<Rid>> index;
    private final ExtendedBufferManager bufferManager;
    private final String filename;
    private final PageFactory.TableType tableType;
    private final String[] columnNames;
    private final String startKey;
    private final String endKey;

    private Iterator<Rid> ridIterator;
    private boolean isOpen;

    public IndexScanOperator(TreeMap<String, List<Rid>> index,
            ExtendedBufferManager bufferManager,
            String filename,
            PageFactory.TableType tableType,
            String[] columnNames,
            String startKey,
            String endKey) {
        this.index = index;
        this.bufferManager = bufferManager;
        this.filename = filename;
        this.tableType = tableType;
        this.columnNames = columnNames;
        this.startKey = startKey;
        this.endKey = endKey;
        this.isOpen = false;
    }

    @Override
    public void open() {
        System.out.println("IndexScanOperator.open() called for range: " + startKey + " to " + endKey);
        System.out.println("Query key: '" + startKey + "' (hex: " + toHex(startKey) + ")");
        System.out.println("Query end key: '" + endKey + "' (hex: " + toHex(endKey) + ")");
        if (isOpen)
            return;
        // Only support MOVIES table for now
        if (tableType != PageFactory.TableType.MOVIES) {
            throw new UnsupportedOperationException("IndexScanOperator only supports MOVIES table");
        }

        // Get RIDs for the range using TreeMap's subMap
        List<Rid> allRids = new ArrayList<>();
        for (List<Rid> rids : index.subMap(startKey, true, endKey, true).values()) {
            allRids.addAll(rids);
        }
        ridIterator = allRids.iterator();

        System.out.println(
                "Index lookup for range '" + startKey + "' to '" + endKey + "' found " + allRids.size() + " RIDs");
        isOpen = true;
    }

    @Override
    public Tuple next() {
        if (!isOpen || ridIterator == null)
            return null;
        int debugCount = 0;
        while (ridIterator.hasNext()) {
            Rid rid = ridIterator.next();
            // Fetch the page and row
            Page page = bufferManager.getPage(filename, rid.getPageId());
            System.out.println("Fetching page " + rid.getPageId() + " from file " + filename +
                    " returned: " + (page == null ? "null" : "valid page"));
            if (page == null) {
                System.out.println("WARNING: Could not fetch page " + rid.getPageId() +
                        " from file " + filename);
                continue;
            }
            Row row = page.getRow(rid.getSlotId());
            bufferManager.unpinPage(filename, rid.getPageId());
            if (row == null)
                continue;
            // Convert Row to Tuple
            String[] values = new String[] {
                    new String(row.movieId).trim(),
                    new String(row.title).trim()
            };
            // Only print the first 10 tuples for debugging
            if (debugCount < 10) {
                debugCount++;
            }
            return new Tuple(values, columnNames);
        }
        return null;
    }

    @Override
    public void close() {
        ridIterator = null;
        isOpen = false;
    }

    private static String toHex(String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            sb.append(String.format("%02x ", (int) c));
        }
        return sb.toString();
    }
}