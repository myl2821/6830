package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private TransactionId _txid;
    private OpIterator _child;
    private boolean _passed;
    private int _cnt;
    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here

        _txid = t;
        _child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here

        Type [] typeAr = { Type.INT_TYPE };
        return new TupleDesc(typeAr);
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here

        _child.open();
        _passed = false;
        _cnt = 0;
        super.open();
    }

    public void close() {
        // some code goes here

        super.close();
        _child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here

        _passed = false;
        _child.rewind();
        _cnt = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here

        if (_passed) {
            return null;
        }

        while (_child.hasNext()) {
            Tuple t = _child.next();
            try {
                Database.getBufferPool().deleteTuple(_txid, t);
                _cnt++;
            } catch (IOException e) {
                throw new DbException("Delete");
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
