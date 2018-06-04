package simpledb;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Op _what;
    private boolean _nogrouping;
    private Stat _nogroupingStat;

    private ConcurrentHashMap<Field, Stat> _map;

    private static final long serialVersionUID = 1L;

    private class Stat {

        public int _count;
        public int _sum;
        public int _min;
        public int _max;

        Stat() {
            _count = 0;
            _sum = 0;
            _min = Integer.MAX_VALUE;
            _max = Integer.MIN_VALUE;
        }

        public void update(IntField field) {
            int val = field.getValue();
            _count++;
            _sum += val;
            if (val < _min) {
                _min = val;
            }
            if (val > _max) {
                _max = val;
            }
        }

        public int emit(Op op) {
            switch (op) {
                case MAX: return _max;
                case MIN: return _min;
                case SUM: return _sum;
                case COUNT: return _count;
                case AVG: return _sum / _count;
                default:
                    // XXX: not implemented for some op
                    return -1;
            }
        }
    }

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here

        _gbfield = gbfield;
        _gbfieldtype = gbfieldtype;
        _afield = afield;
        _what = what;
        _nogrouping = false;
        _map = new ConcurrentHashMap<>();

        if (_gbfield == NO_GROUPING || _gbfieldtype == null) {
            _nogrouping = true;
            _nogroupingStat = new Stat();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        if (_nogrouping) {
            IntField field = (IntField) tup.getField(_afield);
            _nogroupingStat.update(field);
            return;
        }

        Field gbField = tup.getField(_gbfield);
        IntField aField = (IntField) tup.getField(_afield);
        Stat stat = _map.getOrDefault(gbField, new Stat());
        stat.update(aField);
        _map.put(gbField, stat);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here

        return new OpIterator() {
            private boolean _opened = false;
            private Iterator<Map.Entry<Field, Stat>> _iter = null;
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
                    t.setField(0, new IntField(_nogroupingStat.emit(_what)));
                    return t;
                }

                Map.Entry<Field, Stat> entry = _iter.next();
                Tuple t = new Tuple(getTupleDesc());
                t.setField(0, entry.getKey());
                t.setField(1, new IntField(entry.getValue().emit(_what)));

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
