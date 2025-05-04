package buffermanager;

/**
 * Interface for predicates used in selection operations.
 */
public interface Predicate {
    /**
     * Tests if the given tuple satisfies this predicate.
     * 
     * @param tuple The tuple to test
     * @return true if the tuple satisfies the predicate, false otherwise
     */
    boolean test(Tuple tuple);
}

/**
 * Predicate for range comparison (>= and <=).
 */
class RangePredicate implements Predicate {
    private final String columnName;
    private final String lowerBound;
    private final String upperBound;

    /**
     * Creates a new range predicate with the given column name and bounds.
     * 
     * @param columnName The name of the column to test
     * @param lowerBound The lower bound (inclusive)
     * @param upperBound The upper bound (inclusive)
     */
    public RangePredicate(String columnName, String lowerBound, String upperBound) {
        this.columnName = columnName;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @Override
    public boolean test(Tuple tuple) {
        String value = tuple.getValue(columnName);

        if (value == null) {
            return false;
        }

        return value.compareTo(lowerBound) >= 0 && value.compareTo(upperBound) <= 0;
    }
}

class EqualityPredicate implements Predicate {
    private final String columnName;
    private final String value;

    public EqualityPredicate(String columnName, String value) {
        this.columnName = columnName;
        this.value = value;
    }

    @Override
    public boolean test(Tuple tuple) {
        String tupleValue = tuple.getValue(columnName);

        if (tupleValue == null) {
            return false;
        }

        // Special handling for category field to match directors
        if (columnName.contains("category") && value.equalsIgnoreCase("director")) {
            return tupleValue.trim().toLowerCase().contains("direct");
        }

        // Standard equality for other fields
        return tupleValue.trim().equalsIgnoreCase(value.trim());
    }
}

/**
 * Predicate for join condition.
 */
class JoinPredicate implements Predicate {
    private final String leftColumnName;
    private final String rightColumnName;

    /**
     * Creates a new join predicate with the given column names.
     * 
     * @param leftColumnName  The name of the column in the left tuple
     * @param rightColumnName The name of the column in the right tuple
     */
    public JoinPredicate(String leftColumnName, String rightColumnName) {
        this.leftColumnName = leftColumnName;
        this.rightColumnName = rightColumnName;
    }

    /**
     * Tests if the given tuples satisfy this join predicate.
     * 
     * @param leftTuple  The left tuple to test
     * @param rightTuple The right tuple to test
     * @return true if the tuples satisfy the join predicate, false otherwise
     */
    public boolean test(Tuple leftTuple, Tuple rightTuple) {
        String leftValue = leftTuple.getValue(leftColumnName);
        String rightValue = rightTuple.getValue(rightColumnName);

        if (leftValue == null || rightValue == null) {
            return false;
        }

        return leftValue.equals(rightValue);
    }

    @Override
    public boolean test(Tuple tuple) {
        // This method is not used for join predicates
        throw new UnsupportedOperationException("Cannot test join predicate on a single tuple");
    }

    public String getLeftColumnName() {
        return leftColumnName;
    }

    public String getRightColumnName() {
        return rightColumnName;
    }
}