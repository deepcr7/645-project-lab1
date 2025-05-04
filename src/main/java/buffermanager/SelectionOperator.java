package buffermanager;

/**
 * Operator that filters tuples based on a predicate.
 */
public class SelectionOperator implements Operator {
    private final Operator childOperator;
    private final Predicate predicate;

    /**
     * Creates a new selection operator with the given child operator and predicate.
     * 
     * @param childOperator The child operator to get tuples from
     * @param predicate     The predicate to filter tuples with
     */
    public SelectionOperator(Operator childOperator, Predicate predicate) {
        this.childOperator = childOperator;
        this.predicate = predicate;
    }

    @Override
    public void open() {
        // Open the child operator
        childOperator.open();
    }

    @Override
    public Tuple next() {
        // Get tuples from the child operator until we find one that satisfies the
        // predicate
        Tuple tuple;
        while ((tuple = childOperator.next()) != null) {
            if (predicate.test(tuple)) {
                return tuple;
            }
        }

        // If we get here, there are no more tuples that satisfy the predicate
        return null;
    }

    @Override
    public void close() {
        // Close the child operator
        childOperator.close();
    }
}