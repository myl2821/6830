package simpledb;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Op _what;
    private int _cnt;
    private boolean _nogrouping = false;

    private ConcurrentHashMap<Field, Integer> _map;

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here

        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }

        _gbfield = gbfield;
        _gbfieldtype = gbfieldtype;
        _afield = afield;
        _what = what;
        _map = new ConcurrentHashMap<>();
        _cnt = 0;

        if (_gbfield == NO_GROUPING || _gbfieldtype == null) {
            _nogrouping = true;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        if (_nogrouping) {
            _cnt += 1;
            return;
        }

        Field gbField = tup.getField(_gbfield);
        int cnt = _map.getOrDefault(gbField, 0);

        // Only support count
        _map.put(gbField, cnt + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here

        return new OpIterator() {
            private boolean _opened = false;
            private Iterator<Map.Entry<Field, Integer>> _iter = null;
            private boolean _nogroupingFinished;


            @Override
            public void open() throws DbException, TransactionAbortedException {
                _opened = true;
                _iter = _map.entrySet().iterator();
                _nogroupingFinished = false;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!_opened) {
                    throw new IllegalStateException();
                }

                if (_nogrouping) {
                    return !_nogroupingFinished;
                }

                return _iter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!_opened) {
                    throw new IllegalStateException();
                }

                if (_nogrouping) {
                    _nogroupingFinished = true;
                    Type[] typeAr = { Type.INT_TYPE };
                    TupleDesc td = new TupleDesc(typeAr);
                    Tuple t = new Tuple(td);
                    t.setField(0, new IntField(_cnt));
                    return t;
                }

                Map.Entry<Field, Integer> entry = _iter.next();

                Tuple t = new Tuple(getTupleDesc());

                t.setField(0, entry.getKey());

                // Only Count supported
                t.setField(1, new IntField(entry.getValue()));

                return t;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (!_opened) {
                    throw new IllegalStateException();
                }
                close();
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                Type[] typeAr = { _gbfieldtype, Type.INT_TYPE };
                return new TupleDesc(typeAr);
            }

            @Override
            public void close() {
                _opened = false;
                _iter = null;
            }
        };
    }

}
