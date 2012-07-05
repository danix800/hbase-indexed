package org.apache.hadoop.hbase.regionserver.indexed;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;

public class IndexScannerContext {

	private final Iterator<KeyValue> matchedExpressionIterator;

	public IndexScannerContext(List<KeyValue> matchedExpression) {
		this.matchedExpressionIterator = matchedExpression.iterator();
	}

	public KeyValue getNextKey() {
		if (matchedExpressionIterator.hasNext()) {
			return matchedExpressionIterator.next();
		} else {
			return null;
		}
	}
}
