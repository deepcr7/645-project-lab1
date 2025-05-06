package buffermanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Operator that implements a Block Nested Loop Join algorithm.
 */
public class BlockNestedLoopJoinOperator implements Operator {
    private final Operator outerOperator;
    private final Operator innerOperator;
    private final JoinPredicate joinPredicate;
    private final ExtendedBufferManager bufferManager;
    private final int bufferSize;
    private final int specialFileId;

    private boolean isOpen;
    private Map<String, List<Tuple>> hashTable;
    private List<Tuple> currentBlock;
    private List<Page> tempPages;
    private int blockSizeInPages;
    private Tuple currentOuterTuple;
    private Tuple currentInnerTuple;
    private List<Tuple> matchingInnerTuples;
    private int currentInnerTupleIndex;
    private boolean endOfOuterRelation;
    private int currentOuterIndex;
    private int tupleCounter;

    /**
     * Creates a new Block Nested Loop Join operator.
     * 
     * @param outerOperator The operator for the outer (left) relation
     * @param innerOperator The operator for the inner (right) relation
     * @param joinPredicate The predicate for joining tuples
     * @param bufferManager The buffer manager
     * @param bufferSize    The size of the buffer pool
     * @param specialFileId A special file ID for temporary pages
     */
    public BlockNestedLoopJoinOperator(
            Operator outerOperator,
            Operator innerOperator,
            JoinPredicate joinPredicate,
            ExtendedBufferManager bufferManager,
            int bufferSize,
            int specialFileId) {
        this.outerOperator = outerOperator;
        this.innerOperator = innerOperator;
        this.joinPredicate = joinPredicate;
        this.bufferManager = bufferManager;
        this.bufferSize = bufferSize;
        this.specialFileId = specialFileId;

        this.isOpen = false;
        this.hashTable = new HashMap<>();
        this.currentBlock = new ArrayList<>();
        this.tempPages = new ArrayList<>();
        this.blockSizeInPages = (bufferSize - 2) / 2;
        if (this.blockSizeInPages < 1) {
            this.blockSizeInPages = 1;
        }
        this.endOfOuterRelation = false;
        this.currentOuterIndex = 0;
        this.tupleCounter = 0;
    }

    @Override
    public void open() {
        if (isOpen) {
            return;
        }

        outerOperator.open();
        innerOperator.open();

        isOpen = true;
        hashTable.clear();
        currentBlock.clear();
        if (tempPages != null) {
            releaseTemporaryPages();
        }
        tempPages = new ArrayList<>();
        endOfOuterRelation = false;
        currentOuterIndex = 0;
        currentOuterTuple = null;
        currentInnerTuple = null;
        matchingInnerTuples = null;
        currentInnerTupleIndex = 0;

        // Load the first block of the outer relation
        loadNextBlock();
    }

    @Override
    public Tuple next() {
        if (!isOpen) {
            throw new IllegalStateException("Operator not open");
        }

        // If we have matching tuples from the current inner tuple, return the next one
        if (matchingInnerTuples != null && currentInnerTupleIndex < matchingInnerTuples.size()) {
            Tuple joinedTuple = new Tuple(currentOuterTuple, matchingInnerTuples.get(currentInnerTupleIndex));
            currentInnerTupleIndex++;
            return joinedTuple;
        }

        // Get the next matching tuple
        while (true) {
            if (currentInnerTuple == null) {
                // If we've exhausted the current block, load the next block
                if (currentOuterIndex >= currentBlock.size()) {
                    if (endOfOuterRelation) {
                        // We've processed all blocks, so we're done
                        return null;
                    }

                    // Load the next block
                    loadNextBlock();

                    // If there are no more blocks, we're done
                    if (currentBlock.isEmpty()) {
                        return null;
                    }

                    currentOuterIndex = 0; // Reset outer index for the new block
                }

                // Get the next outer tuple from the current block
                currentOuterTuple = currentBlock.get(currentOuterIndex);
                currentOuterIndex++;

                // Reset the inner relation
                innerOperator.close();
                innerOperator.open();

                // Get the first inner tuple
                currentInnerTuple = innerOperator.next();
                currentInnerTupleIndex = 0;
                matchingInnerTuples = new ArrayList<>();
            }

            // Process all inner tuples for the current outer tuple
            while (currentInnerTuple != null) {
                // Check if the tuples match
                if (joinPredicate.test(currentOuterTuple, currentInnerTuple)) {
                    matchingInnerTuples.add(currentInnerTuple);
                }

                // Get the next inner tuple
                currentInnerTuple = innerOperator.next();
            }

            // If we found matching tuples, return the first one
            if (!matchingInnerTuples.isEmpty()) {
                Tuple joinedTuple = new Tuple(currentOuterTuple, matchingInnerTuples.get(0));
                currentInnerTupleIndex = 1;
                return joinedTuple;
            }

            // No matching tuples for this outer tuple, move to the next one
            currentInnerTuple = null;
        }
    }

    @Override
    public void close() {
        if (!isOpen) {
            return;
        }

        outerOperator.close();
        innerOperator.close();

        isOpen = false;
        hashTable.clear();
        currentBlock.clear();

        // Unpin and release all temporary pages
        releaseTemporaryPages();
    }

    /**
     * Loads the next block of tuples from the outer relation into memory
     * and builds a hash table for fast lookup.
     */
    private void loadNextBlock() {
        currentBlock.clear();
        hashTable.clear();

        // Release any existing temporary pages before creating new ones
        releaseTemporaryPages();

        // Create new temporary pages for this block
        createTemporaryBlock();

        int tupleCount = 0;
        int maxTuplesPerBlock = blockSizeInPages * 150; // Increased for better utilization
        Tuple tuple;

        long startTime = System.currentTimeMillis();

        // Load tuples until we fill the block
        while (tupleCount < maxTuplesPerBlock && (tuple = outerOperator.next()) != null) {
            currentBlock.add(tuple);

            // Add the tuple to the hash table - optimize to reuse lists
            String key = tuple.getValue(joinPredicate.getLeftColumnName());
            if (key != null) {
                hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(tuple);
            }

            // Store tuple in temporary block (conceptually)
            storeTupleInTemporaryBlock(tuple, tupleCount);

            tupleCount++;
        }

        // If we didn't get any tuples, we've reached the end of the outer relation
        endOfOuterRelation = (tupleCount == 0);

        // Reset the inner relation variables
        currentInnerTuple = null;
        matchingInnerTuples = null;
        currentInnerTupleIndex = 0;
        currentOuterIndex = 0;

        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Loaded block with " + tupleCount + " tuples, hash table size: " +
                hashTable.size() + " in " + elapsed + "ms");
    }

    /**
     * Creates temporary pages for the current block.
     */
    private void createTemporaryBlock() {
        tempPages = new ArrayList<>();

        for (int i = 0; i < blockSizeInPages; i++) {
            Page tempPage = new PageImpl(i);
            tempPages.add(tempPage);
        }
    }

    /**
     * Stores a tuple in the temporary block.
     * This is a conceptual operation - in reality, we'd serialize the tuple
     * and store it in a temporary page.
     */
    private void storeTupleInTemporaryBlock(Tuple tuple, int tupleIndex) {
        // Calculate which page this tuple belongs to
        int pageIndex = tupleIndex / 100; // Assuming 100 tuples per page

        // Make sure we have enough pages
        while (pageIndex >= tempPages.size() && tempPages.size() < blockSizeInPages) {
            tempPages.add(new PageImpl(tempPages.size()));
        }
    }

    /**
     * Releases all temporary pages used by this operator.
     */
    private void releaseTemporaryPages() {
        tempPages.clear();
    }
}