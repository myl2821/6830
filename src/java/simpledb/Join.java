package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private JoinPredicate _p;
    private OpIterator _child1;
    private OpIterator _child2;
    private TupleDesc _td;
    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here

        _p = p;
        _child1 = child1;
        _child2 = child2;
        _td = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here

        return _p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here

        return _child1.getTupleDesc().getFieldName(_p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here

        return _child1.getTupleDesc().getFieldName(_p.getField1());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here

        return _td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here

        _child1.open();
        _child2.open();
        checkAnyChildEmpty();
        super.open();
    }

    public void close() {
        // some code goes here

        _child1.close();
        _child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here

        _child1.rewind();
        _child2.rewind();
        super.close();
        super.open();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here


        Tuple[] pair = null;
        while((pair = getNextPair()) != null) {
            if (_p.filter(pair[0], pair[1])) {
                Tuple tuple = new Tuple(getTupleDesc());

                int cursor = 0;

                for (Iterator<Field> fields = pair[0].fields(); fields.hasNext(); ) {
                    tuple.setField(cursor++, fields.next());
                }

                for (Iterator<Field> fields = pair[1].fields(); fields.hasNext(); ) {
                    tuple.setField(cursor++, fields.next());
                }

                return tuple;
            }
        }

        return null;
    }

    private Boolean _childEmpty;
    private void checkAnyChildEmpty() throws TransactionAbortedException, DbException {
        _childEmpty = !(_child1.hasNext() && _child2.hasNext());
    }

    private Tuple _t1;
    private Tuple[] getNextPair() throws TransactionAbortedException, DbException {
        if (_childEmpty)
            return null;

        // Initialize
        if (_t1 == null) {
            _t1 = _child1.next();
        }

        if (_child2.hasNext()) {
            Tuple[] pair = {_t1, _child2.next()};
            return pair;
        } else {
            // time to do next loop
            if (!_child1.hasNext()) {
                return null;
            }
            _child2.rewind();
            _t1 = _child1.next();
            Tuple[] pair = {_t1, _child2.next()};

            return pair;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here

        OpIterator[] children = { _child1, _child2 };
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here

        _child1 = children[0];
        _child2 = children[1];
    }
}
