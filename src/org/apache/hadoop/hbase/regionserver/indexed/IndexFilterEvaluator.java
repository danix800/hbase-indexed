package org.apache.hadoop.hbase.regionserver.indexed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.regionserver.IndexedHRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class IndexFilterEvaluator {

	private final static Logger LOG = Logger
			.getLogger(IndexFilterEvaluator.class);

	private IndexedHRegion region;

	public IndexFilterEvaluator(IndexedHRegion region) {
		this.region = region;
	}

	public List<KeyValue> evaluate(Map<Pair, List<StoreFile>> indexMap,
			Filter filter) throws IOException {
		if (filter == null) {
			return null;
		}
		if (filter instanceof FilterList) {
			return evaluate(indexMap, (FilterList) filter);
		} else if (filter instanceof SingleColumnValueFilter) {
			return evaluate(indexMap, (SingleColumnValueFilter) filter);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private List<KeyValue> evaluate(Map<Pair, List<StoreFile>> indexMap,
			FilterList filterList) throws IOException {
		List<KeyValue> result = null;
		if (filterList.getOperator() == FilterList.Operator.MUST_PASS_ALL) {
			for (Filter filter : filterList.getFilters()) {
				List<KeyValue> childResult = evaluate(indexMap, filter);
				if (result == null) {
					result = childResult;
				} else if (childResult != null) {
					result = (ArrayList<KeyValue>) CollectionUtils.intersection(result,
							childResult);
				}
			}
		} else if (filterList.getOperator() == FilterList.Operator.MUST_PASS_ONE) {
			for (Filter filter : filterList.getFilters()) {
				List<KeyValue> childResult = evaluate(indexMap, filter);
				if (result == null) {
					result = childResult;
				} else if (childResult != null) {
					result = (ArrayList<KeyValue>) CollectionUtils.union(result,
							childResult);
				}
			}
		}
		return result;
	}

	private List<KeyValue> evaluate(Map<Pair, List<StoreFile>> indexMap,
			SingleColumnValueFilter singleColumnValueFilter) throws IOException {
		List<StoreFile> sfs = indexMap.get(new Pair(singleColumnValueFilter
				.getFamily(), singleColumnValueFilter.getQualifier()));
		if (sfs == null) {
			LOG.info(String.format("No index for column: '%s', qualifier: '%s'",
					Bytes.toString(singleColumnValueFilter.getFamily()),
					Bytes.toString(singleColumnValueFilter.getQualifier())));
			return null;
		}

		WritableByteArrayComparable comparator = singleColumnValueFilter
				.getComparator();
		byte[] value = comparator.getValue();

		Scan scan = null;
		switch (singleColumnValueFilter.getOperator()) {
		case EQUAL:
			if (comparator instanceof BinaryPrefixComparator) {
				scan = new Scan(value, IndexedTableUtils.incrementAtIndex(value,
						value.length - 1));
				// } else if (comparator instanceof RegexStringComparator) {
				// scan = new Scan();
				// scan.setFilter(new RowFilter(CompareOp.EQUAL, comparator));
			} else {
				scan = new Scan(new Get(value));
			}
			break;
		case NOT_EQUAL:
			scan = new Scan(HConstants.EMPTY_START_ROW, new RowFilter(
					CompareOp.NOT_EQUAL, new BinaryComparator(value)));
			break;
		case GREATER:
			scan = new Scan(value, new RowFilter(CompareOp.NOT_EQUAL,
					new BinaryComparator(value)));
			break;
		case GREATER_OR_EQUAL:
			scan = new Scan(value);
			break;
		case LESS:
			scan = new Scan(HConstants.EMPTY_START_ROW, value);
			break;
		case LESS_OR_EQUAL:
			scan = new Scan(HConstants.EMPTY_START_ROW);
			scan.setFilter(new InclusiveStopFilter(value));
			break;
		case NO_OP:
			scan = new Scan();
			break;
		}
		scan.setMaxVersions();
		InternalScanner scanner = region
				.instantiateIndexStoreFileScanner(sfs, scan);
		List<KeyValue> matched = new ArrayList<KeyValue>();
		List<KeyValue> nextRows = new ArrayList<KeyValue>();
		boolean moreRows;
		do {
			moreRows = scanner.next(nextRows);
			for (KeyValue keyValue : nextRows) {
				byte[] key = IndexedTableUtils.extractKey(keyValue);
				KeyValue rowKeyValue = KeyValue.createFirstOnRow(key);
				matched.add(rowKeyValue);
			}
			nextRows.clear();
		} while (moreRows);
		return matched;
	}
}
