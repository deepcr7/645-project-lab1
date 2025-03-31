package buffermanager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

public class PerformanceTest {
    private static final String DATA_FILE = "imdb_database.bin";
    private static final String TITLE_INDEX_FILE = "imdb_title_index.bin";
    private static final String MOVIEID_INDEX_FILE = "imdb_movieid_index.bin";
    private static final int BUFFER_SIZE = 50000;
    private static final boolean USE_SAMPLE_DATA = true;
    private static final int SAMPLE_SIZE = 10000;

    private static ExtendedBufferManager bufferManager;
    private static BTree<String, Rid> titleIndex;
    private static BTree<String, Rid> movieIdIndex;

    public static void main(String[] args) {
        setup();

        runPerformanceTest1();
        runPerformanceTest2();
        runPerformanceTest3();

        cleanup();
    }

    private static void setup() {
        bufferManager = new ExtendedBufferManagerImpl(BUFFER_SIZE);
        File titleIndexFile = new File(TITLE_INDEX_FILE);
        File movieIdIndexFile = new File(MOVIEID_INDEX_FILE);

        if (!titleIndexFile.exists() || !movieIdIndexFile.exists()) {
            titleIndex = new BTreeImpl<>(bufferManager, TITLE_INDEX_FILE);
            movieIdIndex = new BTreeImpl<>(bufferManager, MOVIEID_INDEX_FILE);

            buildIndexes();
        } else {

            titleIndex = new BTreeImpl<>(bufferManager, TITLE_INDEX_FILE);
            movieIdIndex = new BTreeImpl<>(bufferManager, MOVIEID_INDEX_FILE);
        }
    }

    private static void buildIndexes() {
        File dataFile = new File(DATA_FILE);
        int totalPages = (int) (dataFile.length() / PageImpl.PAGE_SIZE);
        for (int pageId = 0; pageId < totalPages; pageId++) {
            Page page = bufferManager.getPage(DATA_FILE, pageId);

            if (page != null) {
                int rows = getRowCount(page);

                for (int rowId = 0; rowId < rows; rowId++) {
                    Row row = page.getRow(rowId);

                    if (row != null) {
                        String title = new String(row.title).trim();
                        Rid rid = new Rid(pageId, rowId);

                        titleIndex.insert(title, rid);
                    }
                }

                bufferManager.unpinPage(DATA_FILE, pageId);
            }
        }
        ((BTreeImpl<String>) movieIdIndex).startBulkLoading();

        for (int pageId = 0; pageId < totalPages; pageId++) {
            Page page = bufferManager.getPage(DATA_FILE, pageId);

            if (page != null) {
                int rows = getRowCount(page);

                for (int rowId = 0; rowId < rows; rowId++) {
                    Row row = page.getRow(rowId);

                    if (row != null) {
                        String movieId = new String(row.movieId).trim();
                        Rid rid = new Rid(pageId, rowId);

                        movieIdIndex.insert(movieId, rid);
                    }
                }

                bufferManager.unpinPage(DATA_FILE, pageId);
            }
        }

        ((BTreeImpl<String>) movieIdIndex).endBulkLoading();

    }

    private static int getRowCount(Page page) {
        int count = 0;
        while (page.getRow(count) != null) {
            count++;
        }
        return count;
    }

    private static void runPerformanceTest1() {
        double[] selectivityLevels = USE_SAMPLE_DATA ? new double[] { 0.01, 0.05, 0.1, 0.2, 0.5 }
                : new double[] { 0.001, 0.01, 0.05, 0.1, 0.2 };

        try (FileWriter writer = new FileWriter("p1_results.csv")) {
            writer.write("Selectivity,DirectScan(ms),IndexScan(ms),Ratio\n");

            for (double selectivity : selectivityLevels) {
                String[] range = generateTitleRange(selectivity);
                String startTitle = range[0];
                String endTitle = range[1];
                if (bufferManager instanceof ExtendedBufferManagerImpl) {
                    ((ExtendedBufferManagerImpl) bufferManager).freeUpBufferSpace();
                }
                long directScanStart = System.currentTimeMillis();
                List<Row> directScanResults = directScanByTitle(startTitle, endTitle, selectivity > 0.1 ? 5 : 1);
                long directScanEnd = System.currentTimeMillis();
                long directScanTime = directScanEnd - directScanStart;
                if (bufferManager instanceof ExtendedBufferManagerImpl) {
                    ((ExtendedBufferManagerImpl) bufferManager).freeUpBufferSpace();
                }
                long indexScanStart = System.currentTimeMillis();
                List<Row> indexScanResults = indexScanByTitle(startTitle, endTitle);
                long indexScanEnd = System.currentTimeMillis();
                long indexScanTime = indexScanEnd - indexScanStart;

                System.out.println("Index scan found " + indexScanResults.size() +
                        " rows in " + indexScanTime + "ms");
                double ratio = (directScanTime > 0 && indexScanTime > 0) ? (double) directScanTime / indexScanTime : 0;

                writer.write(selectivity + "," + directScanTime + "," +
                        indexScanTime + "," + ratio + "\n");
            }
        } catch (IOException e) {
            System.err.println("Error writing results: " + e.getMessage());
        }
    }

    private static void runPerformanceTest2() {
        double[] selectivityLevels = USE_SAMPLE_DATA ? new double[] { 0.01, 0.05, 0.1, 0.2, 0.5 }
                : new double[] { 0.001, 0.01, 0.05, 0.1, 0.2 };

        try (FileWriter writer = new FileWriter("p2_results.csv")) {
            writer.write("Selectivity,DirectScan(ms),IndexScan(ms),Ratio\n");

            for (double selectivity : selectivityLevels) {
                String[] range = generateMovieIdRange(selectivity);
                String startMovieId = range[0];
                String endMovieId = range[1];
                long directScanStart = System.currentTimeMillis();
                List<Row> directScanResults = directScanByMovieId(startMovieId, endMovieId);
                long directScanEnd = System.currentTimeMillis();
                long directScanTime = directScanEnd - directScanStart;

                System.out
                        .println("Direct scan found " + directScanResults.size() + " rows in " + directScanTime + "ms");
                long indexScanStart = System.currentTimeMillis();
                List<Row> indexScanResults = indexScanByMovieId(startMovieId, endMovieId);
                long indexScanEnd = System.currentTimeMillis();
                long indexScanTime = indexScanEnd - indexScanStart;
                double ratio = (double) directScanTime / indexScanTime;

                writer.write(selectivity + "," + directScanTime + "," + indexScanTime + "," + ratio + "\n");
            }
        } catch (IOException e) {
            System.err.println("Error writing results: " + e.getMessage());
        }
    }

    private static void runPerformanceTest3() {
        double[] selectivityLevels = { 0.001, 0.01, 0.05, 0.1, 0.2 };

        try (FileWriter writer = new FileWriter("p3_results.csv")) {
            writer.write("Selectivity,TitleIndex(ms),MovieIdIndex(ms)\n");
            pinFirstLevels();

            for (double selectivity : selectivityLevels) {
                String[] titleRange = generateTitleRange(selectivity);
                String startTitle = titleRange[0];
                String endTitle = titleRange[1];

                String[] movieIdRange = generateMovieIdRange(selectivity);
                String startMovieId = movieIdRange[0];
                String endMovieId = movieIdRange[1];
                long titleIndexStart = System.currentTimeMillis();
                List<Row> titleResults = indexScanByTitle(startTitle, endTitle);
                long titleIndexEnd = System.currentTimeMillis();
                long titleIndexTime = titleIndexEnd - titleIndexStart;

                System.out.println("Title index scan found " + titleResults.size() +
                        " rows in " + titleIndexTime + "ms (with pinned levels)");
                long movieIdIndexStart = System.currentTimeMillis();
                List<Row> movieIdResults = indexScanByMovieId(startMovieId, endMovieId);
                long movieIdIndexEnd = System.currentTimeMillis();
                long movieIdIndexTime = movieIdIndexEnd - movieIdIndexStart;

                System.out.println("MovieId index scan found " + movieIdResults.size() +
                        " rows in " + movieIdIndexTime + "ms (with pinned levels)");

                writer.write(selectivity + "," + titleIndexTime + "," + movieIdIndexTime + "\n");
            }
        } catch (IOException e) {
            System.err.println("Error writing results: " + e.getMessage());
        }
    }

    private static void pinFirstLevels() {

        try {
            Page titleRoot = bufferManager.getPage(TITLE_INDEX_FILE, 0);
            Page movieIdRoot = bufferManager.getPage(MOVIEID_INDEX_FILE, 0);

            if (titleRoot == null || movieIdRoot == null) {
                return;
            }
            if (bufferManager instanceof ExtendedBufferManagerImpl) {
                ((ExtendedBufferManagerImpl) bufferManager).freeUpBufferSpace();
            }
            BiConsumer<String, Integer> pinChildren = (filename, nodeId) -> {
                try {
                    Page nodePage = bufferManager.getPage(filename, nodeId);
                    if (nodePage == null)
                        return;
                    byte[] data = nodePage.getData();
                    boolean isLeaf = data[4] == 1;

                    if (!isLeaf) {
                        ByteBuffer buffer = ByteBuffer.wrap(data);
                        buffer.position(12);
                        int keyCount = buffer.getInt();
                        List<Integer> childrenIds = new ArrayList<>();
                        for (int i = 0; i < keyCount; i++) {
                            int keyLength = buffer.getInt();
                            buffer.position(buffer.position() + keyLength);
                            int childId = buffer.getInt();
                            childrenIds.add(childId);
                        }
                        if (keyCount > 0) {
                            int lastChildId = buffer.getInt();
                            childrenIds.add(lastChildId);
                        }
                        for (Integer childId : childrenIds) {
                            Page childPage = bufferManager.getPage(filename, childId);
                            if (childPage != null) {
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error pinning children: " + e.getMessage());
                }
            };
            pinChildren.accept(TITLE_INDEX_FILE, 0);
            pinChildren.accept(MOVIEID_INDEX_FILE, 0);

        } catch (Exception e) {
            System.err.println("Error pinning first levels: " + e.getMessage());
        }
    }

    private static List<Row> directScanByTitle(String startTitle, String endTitle, int sampleEvery) {
        List<Row> results = new ArrayList<>();
        File dataFile = new File(DATA_FILE);
        int totalPages = (int) (dataFile.length() / PageImpl.PAGE_SIZE);
        int maxPages = USE_SAMPLE_DATA ? (SAMPLE_SIZE / 10) + 1 : totalPages;
        for (int pageId = 0; pageId < maxPages; pageId++) {
            if (sampleEvery > 1 && pageId % sampleEvery != 0) {
                continue;
            }

            Page page = bufferManager.getPage(DATA_FILE, pageId);

            if (page != null) {
                int rows = getRowCount(page);

                for (int rowId = 0; rowId < rows; rowId++) {
                    Row row = page.getRow(rowId);

                    if (row != null) {
                        String title = new String(row.title).trim();

                        if (title.compareTo(startTitle) >= 0 && title.compareTo(endTitle) <= 0) {
                            results.add(new Row(new String(row.movieId), title));
                        }
                    }
                }

                bufferManager.unpinPage(DATA_FILE, pageId);
                if (pageId % 1000 == 0) {
                    if (bufferManager instanceof ExtendedBufferManagerImpl) {
                        ((ExtendedBufferManagerImpl) bufferManager).freeUpBufferSpace();
                    }
                }
            }
        }

        return results;
    }

    private static List<Row> directScanByMovieId(String startMovieId, String endMovieId) {
        List<Row> results = new ArrayList<>();
        File dataFile = new File(DATA_FILE);
        int totalPages = (int) (dataFile.length() / PageImpl.PAGE_SIZE);
        for (int pageId = 0; pageId < totalPages; pageId++) {
            Page page = bufferManager.getPage(DATA_FILE, pageId);

            if (page != null) {
                int rows = getRowCount(page);

                for (int rowId = 0; rowId < rows; rowId++) {
                    Row row = page.getRow(rowId);

                    if (row != null) {
                        String movieId = new String(row.movieId).trim();

                        if (movieId.compareTo(startMovieId) >= 0 && movieId.compareTo(endMovieId) <= 0) {
                            results.add(new Row(movieId, new String(row.title)));
                        }
                    }
                }

                bufferManager.unpinPage(DATA_FILE, pageId);
            }
        }

        return results;
    }

    private static List<Row> indexScanByTitle(String startTitle, String endTitle) {
        List<Row> results = new ArrayList<>();

        Iterator<Rid> ridIter = titleIndex.rangeSearch(startTitle, endTitle);

        while (ridIter.hasNext()) {
            Rid rid = ridIter.next();

            Page page = bufferManager.getPage(DATA_FILE, rid.getPageId());

            if (page != null) {
                Row row = page.getRow(rid.getSlotId());

                if (row != null) {
                    results.add(new Row(new String(row.movieId), new String(row.title)));
                }

                bufferManager.unpinPage(DATA_FILE, rid.getPageId());
            }
        }

        return results;
    }

    private static List<Row> indexScanByMovieId(String startMovieId, String endMovieId) {
        List<Row> results = new ArrayList<>();

        Iterator<Rid> ridIter = movieIdIndex.rangeSearch(startMovieId, endMovieId);

        while (ridIter.hasNext()) {
            Rid rid = ridIter.next();

            Page page = bufferManager.getPage(DATA_FILE, rid.getPageId());

            if (page != null) {
                Row row = page.getRow(rid.getSlotId());

                if (row != null) {
                    results.add(new Row(new String(row.movieId), new String(row.title)));
                }

                bufferManager.unpinPage(DATA_FILE, rid.getPageId());
            }
        }

        return results;
    }

    private static String[] generateTitleRange(double selectivity) {

        if (selectivity <= 0.01) {
            return new String[] { "A", "Ba" };
        } else if (selectivity <= 0.05) {
            return new String[] { "A", "Da" };
        } else if (selectivity <= 0.1) {
            return new String[] { "A", "Ga" };
        } else if (selectivity <= 0.2) {
            return new String[] { "A", "Ma" };
        } else if (selectivity <= 0.5) {
            return new String[] { "A", "Ta" };
        } else {
            return new String[] { "A", "Zz" };
        }
    }

    private static String[] generateMovieIdRange(double selectivity) {
        int maxId = 10000000;
        int range = (int) (maxId * selectivity);

        return new String[] {
                "tt0000000",
                String.format("tt%07d", range)
        };
    }

    private static void cleanup() {
        bufferManager.force(DATA_FILE);
        bufferManager.force(TITLE_INDEX_FILE);
        bufferManager.force(MOVIEID_INDEX_FILE);
    }
}