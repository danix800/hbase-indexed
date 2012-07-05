hbase-indexed (HBaseIdx)
=============

HBase secondary index using coprocessors

There are several projects providing secondary index for HBase.
This is just one of them.

The HBase community has been talking about secondary index using
coprocessors for a period of time, but no much progress has been
made. This project is the product of my non-busy time.

Features:

	0.92 is supported.
	region level index.
	all index management hooked to coprocessors.
	memstore not indexed.
	inverted index on byte array.
	index stored in HFile.
	scan with Filter.

So why this one while there are several choices?

Like IndexedHBase (IHBase), HBaseIdx indexes all rows on region
level. RegionObserver per HRegion provides extension point for
all those required index management operations. It is non-
intrusive extension. There is a EndPoint provided for manually
indexing a specific region, too.

Tables that need secondary index can be created with a utility
class. You just tell the columns need indexing.

	HColumnDescriptor colDesc = new HColumnDescriptor(fam1);
	IndexedTableUtils.addIndexDescriptor(colDesc,
			new IndexDescriptor(qual1));
	IndexedTableUtils.addIndexDescriptor(colDesc,
			new IndexDescriptor(qual2));
	HTableDescriptor htd = new HTableDescriptor(tableName);
	htd.addFamily(colDesc);

	hAdmin.createTable(htd);

The indexes are only built when a region flushes (like IHBase),
and indexes are written to HFiles.

Indexed scans are based on SingleColumnValueFilter. If a column
is indexed, and an attribute of scan is set, HBaseIdx can query
on the index HFiles for speeding up.

	HTable table = ...
	Scan scan = new Scan();
	Filter f1 = new SingleColumnValueFilter(fam1, qual1,
			CompareOp.EQUAL, Bytes.toBytes(val1));
	Filter f1 = new SingleColumnValueFilter(fam1, qual2,
			CompareOp.EQUAL, Bytes.toBytes(val2));
	Filter f = new FilterList(Operator.MUST_PASS_ALL, f1, f2);
	scan.setFilter(f);
	IndexedTableUtils.setIndexedScan(scan, true);

	ResultScanner scanner = table.getScanner(scan);
	Result r;
	while((r = scanner.next()) != null) {
		// ...
	}

Only BinaryComparator and BinaryPrefixComparator are supported.
For the default BinaryComparator, all CompareOp are valid.
