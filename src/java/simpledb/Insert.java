package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private TransactionId _txid;
    private OpIterator _child;
    private int _tableId;
    private int _cnt;
    private boolean _passed;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here

        _txid = t;
        _tableId = tableId;
        _child = child;
        _passed = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here

        Type [] typeAr = { Type.INT_TYPE };
        return new TupleDesc(typeAr);
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here

        _child.open();
        _cnt = 0;
        _passed = false;
        super.open();
    }

    public void close() {
        // some code goes here

        super.close();
        _child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here

        _child.rewind();
        _cnt = 0;
        _passed = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here

        if (_passed) {
            return null;
        }

        while (_child.hasNext()) {
            Tuple t = _child.next();
            try {
                Database.getBufferPool().insertTuple(_txid, _tableId, t);
                _cnt += 1;


            } catch (IOException e)  {
                throw new DbException("Insert");
            }
        }

        TupleDesc td = getTupleDesc();
        Tuple tuple = new Tuple(td);
        tuple.setField(0, new IntField(_cnt));
        _passed = true;
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here

        OpIterator[] children = { _child };
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here

        _child = children[0];
    }
}
