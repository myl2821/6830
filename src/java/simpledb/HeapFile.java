package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File _f;
    private TupleDesc _td;
    private final int _pageSize;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here

        _f = f;
        _td = td;
        _pageSize = BufferPool.getPageSize();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here

        return _f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here

        return _f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here

       return _td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here

        // table of page must be identical
        if (getId() != pid.getTableId()) {
            return null;
        }

        int pn = pid.getPageNumber();

        if (pn < 0 || pn >=numPages()) {
            return null;
        }

        try {
            byte[] byteStream = new byte[_pageSize];
            RandomAccessFile raf = new RandomAccessFile(_f, "r");
            raf.seek(_pageSize * pn);
            raf.readFully(byteStream);
            raf.close();
            return new HeapPage(new HeapPageId(pid.getTableId(), pn), byteStream);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1

        HeapPage hp = (HeapPage) page;
        RandomAccessFile raf = new RandomAccessFile(_f, "rw");
        raf.seek(BufferPool.getPageSize() * page.getId().getPageNumber());
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here

        long fl = _f.length();

        return (int)(fl + _pageSize - 1)/ _pageSize;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here

        if (!t.getTupleDesc().equals(_td)) {
            throw new DbException("deleteTuple");
        }

        int pn;

        for (pn = 0; pn < numPages(); pn++) {
            // try to find an empty slot to insert tuple
            HeapPage hp = (HeapPage)(Database.getBufferPool().getPage(tid,
                    new HeapPageId(getId(), pn),Permissions.READ_ONLY));

            if (hp.getNumEmptySlots() > 0) break;
        }

        if (pn == numPages()) {
            // All pages are full, firstly Create an EmptyPage
            HeapPageId pgId = new HeapPageId(getId(), pn);
            HeapPage newPage = new HeapPage(pgId, HeapPage.createEmptyPageData());
            writePage(newPage);
        }

        // The page to insert tuple
        HeapPage hp = (HeapPage)(Database.getBufferPool().getPage(tid,
                    new HeapPageId(getId(), pn),Permissions.READ_WRITE));

        hp.insertTuple(t);

        ArrayList<Page> pList = new ArrayList<>();
        pList.add(hp);
        return pList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here

        int tbId = t.getRecordId().getPageId().getTableId();
        int pn = t.getRecordId().getPageId().getPageNumber();
        if (getId() != tbId || pn < 0 || pn >= numPages()) {
            throw new DbException("deleteTuple");
        }

        HeapPage hp = (HeapPage)(Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(),
                Permissions.READ_WRITE));
        hp.deleteTuple(t);

        ArrayList<Page> pList = new ArrayList<>();
        pList.add(hp);
        return pList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HeapFileIterator(this, tid);
    }

    public class HeapFileIterator implements DbFileIterator {
        private HeapFile _hf;
        private TransactionId _tid;

        private boolean _opened;
        private int _currentPageIndex;
        private Iterator<Tuple> _currentTupleIter;

        public HeapFileIterator(HeapFile hf, TransactionId tid) {
            _hf = hf;
            _tid = tid;
            close();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (_opened)
                return;

            if(_hf.numPages() <= 0) {
                throw new DbException("Invalid HeapFile");
            }

            _opened = true;
            openNextPage();
        }

        private void openNextPage() throws NoSuchElementException {
            if (_currentPageIndex >= _pageSize) {
                throw new NoSuchElementException();
            }

            PageId pid = new HeapPageId(_hf.getId(), _currentPageIndex);
            try {
                Page p = Database.getBufferPool()
                        .getPage(_tid, pid, Permissions.READ_ONLY);
                _currentTupleIter = ((HeapPage)p).iterator();
                _currentPageIndex++;

            } catch (Exception e) {
                e.printStackTrace();
                throw new NoSuchElementException();
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!_opened)
                return false;

            if (_currentTupleIter != null && _currentTupleIter.hasNext())
                return true;

            // Open next Page
            // N.B. there may be blank page between tuples as a hole
            // due to delete
            while (_currentPageIndex < _hf.numPages()) {
                PageId pid = new HeapPageId(_hf.getId(), _currentPageIndex);
                HeapPage p = (HeapPage) Database.getBufferPool()
                        .getPage(_tid, pid, Permissions.READ_ONLY);
                if (p.iterator().hasNext()) {
                    return true;
                }
                _currentPageIndex++;
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!_opened) {
                throw new NoSuchElementException();
            }

            if (_currentTupleIter == null || !_currentTupleIter.hasNext()) {
                openNextPage();
            }

            return _currentTupleIter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            _currentTupleIter = null;
            _currentPageIndex = 0;
            _opened = false;
        }
    }
}

