#!/bin/bash

ant runtest -Dtest=TupleTest
ant runtest -Dtest=TupleDescTest
ant runtest -Dtest=CatalogTest
ant runtest -Dtest=HeapPageIdTest
ant runtest -Dtest=RecordIdTest
ant runtest -Dtest=HeapPageReadTest
ant runtest -Dtest=HeapFileReadTest
ant runsystest -Dtest=ScanTest
