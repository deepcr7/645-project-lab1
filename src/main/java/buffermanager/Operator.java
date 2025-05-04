package buffermanager;

/**
 * Interface defining the basic operator for query execution using the Iterator
 * model.
 */
public interface Operator {
    /**
     * Initializes the operator for execution.
     */
    void open();

    /**
     * Returns the next tuple from this operator.
     * 
     * @return The next tuple, or null if no more tuples are available
     */
    Tuple next();

    /**
     * Releases any resources held by this operator.
     */
    void close();
}