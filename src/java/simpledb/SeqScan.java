package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {
    private TransactionId _tid;
    private int _tableid;
    private String _tableAlias;
    private boolean _opened;
    private DbFileIterator _iter;

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here

        _tid = tid;
        _tableid = tableid;
        _tableAlias = tableAlias;
        _iter = Database.getCatalog().getDatabaseFile(_tableid).iterator(_tid);
        _opened = false;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        // some code goes here

        return Database.getCatalog().getTableName(_tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here

        return _tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here

        if (tableAlias == null) {
            tableAlias = "null";
        }

        _tableid = tableid;
        _tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        if (_opened) return;

        _iter.open();
        _opened = true;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here

        TupleDesc td = Database.getCatalog().getTupleDesc(_tableid);
        int numFields = td.numFields();
        Type[] typeAr = new Type[numFields];
        String[] fieldAr = new String[numFields];


        for (int i = 0; i < numFields; i++) {
            typeAr[i] = td.getFieldType(i);
            String suffix = td.getFieldName(i) == null ? "null" : td.getFieldName(i);
            fieldAr[i] = _tableAlias + "." + suffix;
        }

        return new TupleDesc(typeAr, fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here

        if (!_opened) {
            throw new IllegalStateException();
        }

        return _iter.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here

        if (!_opened) {
            throw new IllegalStateException();
        }

        return _iter.next();
    }

    public void close() {
        // some code goes here

        _iter.close();
        _opened = false;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        if (!_opened) {
            throw new IllegalStateException();
        }

        _iter.rewind();
    }
}
