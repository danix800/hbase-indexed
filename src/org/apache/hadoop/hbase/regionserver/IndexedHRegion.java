/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.regionserver.indexed.IndexFilterEvaluator;
import org.apache.hadoop.hbase.regionserver.indexed.IndexManagerImpl;
import org.apache.hadoop.hbase.regionserver.indexed.IndexScannerContext;
import org.apache.hadoop.hbase.regionserver.indexed.Pair;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;

/**
 * Wrapped HRegion. The customized {@link IndexedRegionScanner} is from IHBase.
 * 
 * @author danis
 * 
 */
public class IndexedHRegion extends HRegion {

	private AtomicInteger numberOfOngoingIndexedScans;
	private AtomicLong totalIndexedScans;
	private HRegion region;

	@SuppressWarnings("unchecked")
	public IndexedHRegion(HRegion region) {
		super();
		this.region = region;
		numberOfOngoingIndexedScans = new AtomicInteger(0);
		totalIndexedScans = new AtomicLong(0);
		try {
			Field field = region.getClass().getDeclaredField(
					"scannerReadPoints");
			field.setAccessible(true);
			this.scannerReadPoints = (ConcurrentHashMap<RegionScanner, Long>) field
					.get(region);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Instantiate a customized {@link RegionScanner}. StoreScanners are
	 * constructed outside of it.
	 * 
	 * @param scan
	 * @param indexScannerContext
	 * @return
	 * @throws IOException
	 */
	public RegionScanner instantiateRegionScanner(Scan scan,
			IndexScannerContext indexScannerContext) throws IOException {
		List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
		for (Map.Entry<byte[], NavigableSet<byte[]>> entry : scan
				.getFamilyMap().entrySet()) {
			Store store = region.getStores().get(entry.getKey());
			StoreScanner scanner = store.getScanner(scan, entry.getValue());
			scanners.add(scanner);
		}
		return new IndexedRegionScanner(scan, scanners, indexScannerContext);
	}

	/**
	 * Instantiate InternalScanner for index store files, used internally by
	 * {@link IndexFilterEvaluator}.
	 * 
	 * @param storeFiles
	 * @param scan
	 * @return
	 * @throws IOException
	 */
	public InternalScanner instantiateIndexStoreFileScanner(
			Collection<StoreFile> storeFiles, Scan scan) throws IOException {
		List<StoreFileScanner> sfScanners = StoreFileScanner
				.getScannersForStoreFiles(storeFiles, false, false, false);
		List<KeyValueScanner> kvs = new ArrayList<KeyValueScanner>(
				sfScanners.size() + 1);

		kvs.addAll(sfScanners);
		List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
		scanners.add(new StoreScanner(scan, new byte[] {}, Long.MAX_VALUE,
				region.getRegionInfo().getComparator(), null, kvs));
		return new IndexedRegionScanner(scan, scanners, null);
	}

	public static StoreFile createStoreFile(final FileSystem fs, final Path p,
			final Configuration conf, final CacheConfig cacheConf,
			final BloomType cfBloomType) throws IOException {
		return new StoreFile(fs, p, conf, cacheConf, StoreFile.BloomType.NONE);
	}

	public InternalScanner createCollectionBackedScanner(List<KeyValue> kvs)
			throws IOException {
		List<? extends KeyValueScanner> cbs = Collections
				.singletonList(new CollectionBackedScanner(kvs, region
						.getRegionInfo().getComparator()));
		Scan scan = new Scan();
		StoreScanner scanner = new IndexedStoreScanner(scan, new byte[] {},
				Integer.MAX_VALUE, region.getRegionInfo().getComparator(),
				null, new ArrayList<KeyValueScanner>(cbs));
		return scanner;
	}

	/**
	 * Used when flushing, we need to flush with deletes.
	 * 
	 * @param scan
	 * @return
	 * @throws IOException
	 */
	public InternalScanner getMemstoreScannerWithDeletes(Scan scan)
			throws IOException {
		List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
		for (byte[] family : region.getTableDesc().getFamiliesKeys()) {
			Store store = region.getStores().get(family);
			List<KeyValueScanner> memstoreScanners = getMemstoreScanners(store,
					scan.getStartRow());
			StoreScanner scanner = new IndexedStoreScanner(scan, new byte[] {},
					Integer.MAX_VALUE, region.getRegionInfo().getComparator(),
					scan.getFamilyMap().get(family), memstoreScanners);
			scanners.add(scanner);
		}
		return new IndexedRegionScanner(scan, scanners, null);
	}

	private class IndexedStoreScanner extends StoreScanner {
		public IndexedStoreScanner(final Scan scan, final byte[] colFamily,
				final long ttl, final KeyValue.KVComparator comparator,
				final NavigableSet<byte[]> columns,
				final List<KeyValueScanner> scanners) throws IOException {
			super(scan, new byte[] {}, Integer.MAX_VALUE, region
					.getRegionInfo().getComparator(), columns, scanners);
			try {
				Field field = StoreScanner.class.getDeclaredField("matcher");
				field.setAccessible(true);
				ScanQueryMatcher matcher = (ScanQueryMatcher) field.get(this);
				matcher.retainDeletesInOutput = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private List<KeyValueScanner> getMemstoreScanners(Store store,
			byte[] startRow) throws IOException {
		List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
		List<KeyValueScanner> memstoreScanners = store.memstore.getScanners();
		KeyValue seekTo = KeyValue.createFirstOnRow(startRow);
		for (int i = memstoreScanners.size() - 1; i >= 0; i--) {
			memstoreScanners.get(i).seek(seekTo);
			scanners.add(memstoreScanners.get(i));
		}
		return scanners;
	}

	public static Scan createInteranlFilesOnlyScan(Set<Pair> columns) {
		InternalScan scan = new InternalScan(new Get());
		scan.setStartRow(HConstants.EMPTY_START_ROW);
		scan.setStopRow(HConstants.EMPTY_END_ROW);
		for (Pair column : columns) {
			scan.addColumn(column.getFirst(), column.getSecond());
		}
		scan.checkOnlyStoreFiles();
		return scan;
	}

	public int averageNumberOfMemStoreKeys() {
		int totalKVs = 0;
		for (Store store : region.getStores().values()) {
			totalKVs += memstoreNumKeyValues(store.memstore);
		}
		return totalKVs / region.getStores().size();
	}

	private int memstoreNumKeyValues(MemStore ms) {
		return ms.kvset.size() + ms.snapshot.size();
	}

	public int getNumberOfOngoingIndexedScans() {
		return numberOfOngoingIndexedScans.get();
	}

	public long getTotalIndexedScans() {
		return totalIndexedScans.get();
	}

	public long resetTotalIndexedScans() {
		return totalIndexedScans.getAndSet(0);
	}

	public void sortStoreFiles(List<StoreFile> files) {
		Collections.sort(files, StoreFile.Comparators.FLUSH_TIME);
	}

	public StoreFile compactIndexFiles(List<StoreFile> filesToCompact,
			Store store, byte[] col) throws IOException {
		if (isClosing() || isClosed()) {
			LOG.debug("Skipping compacting index on " + region
					+ " because closing/closed");
			return null;
		}
		long maxId = StoreFile.getMaxSequenceIdInList(filesToCompact);
		StoreFile.Writer writer = compactStore(filesToCompact, store, maxId);
		StoreFile sf = completeCompaction(store, filesToCompact, writer, col);
		return sf;
	}

	StoreFile completeCompaction(Store store,
			final Collection<StoreFile> compactedFiles,
			final StoreFile.Writer compactedFile, byte[] col)
			throws IOException {
		StoreFile result = null;
		if (compactedFile != null) {
			Path origPath = compactedFile.getPath();
			Path destPath = new Path(store.getHomedir(),
					IndexManagerImpl.INDEX_DIR + "/" + Bytes.toString(col)
							+ "/" + origPath.getName());
			if (!getFilesystem().rename(origPath, destPath)) {
				LOG.error("Failed move of compacted file " + origPath + " to "
						+ destPath);
				throw new IOException("Failed move of compacted file "
						+ origPath + " to " + destPath);
			}
			result = new StoreFile(getFilesystem(), destPath, getConf(),
					store.cacheConf, BloomType.NONE);
			result.createReader();
		}
		return result;
	}

	StoreFile.Writer compactStore(final Collection<StoreFile> filesToCompact,
			Store store, long maxId) throws IOException {
		List<StoreFileScanner> scanners = StoreFileScanner
				.getScannersForStoreFiles(filesToCompact, false, false, true);

		List<KeyValueScanner> kvscanner = new ArrayList<KeyValueScanner>();
		kvscanner.addAll(scanners);
		StoreFile.Writer writer = null;
		try {
			InternalScanner scanner = null;
			try {
				Scan scan = new Scan();
				scan.setMaxVersions(Integer.MAX_VALUE);
				scanner = new StoreScanner(scan, new byte[] {},
						Integer.MAX_VALUE, region.getRegionInfo()
								.getComparator(), null, kvscanner);
				ArrayList<KeyValue> kvs = new ArrayList<KeyValue>();
				boolean hasMore;
				do {
					hasMore = scanner.next(kvs, 10);
					if (writer == null && !kvs.isEmpty()) {
						writer = StoreFile.createWriter(region.getFilesystem(),
								region.getTmpDir(), HFile.DEFAULT_BLOCKSIZE,
								region.getConf(), store.getCacheConfig());
					}
					if (writer != null) {
						for (KeyValue kv : kvs) {
							writer.append(kv);
						}
					}
					kvs.clear();
				} while (hasMore);
			} finally {
				if (scanner != null) {
					scanner.close();
				}
			}
		} finally {
			if (writer != null) {
				writer.appendMetadata(maxId, true);
				writer.close();
			}
		}
		return writer;
	}

	private ConcurrentHashMap<RegionScanner, Long> scannerReadPoints;

	private Method startRegionOperation;

	private void startRegionOperation() {
		try {
			startRegionOperation = region.getClass().getDeclaredMethod(
					"startRegionOperation");
			startRegionOperation.setAccessible(true);
			startRegionOperation.invoke(region);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Method closeRegionOperation;

	private void closeRegionOperation() {
		try {
			closeRegionOperation = region.getClass().getDeclaredMethod(
					"closeRegionOperation");
			closeRegionOperation.setAccessible(true);
			closeRegionOperation.invoke(region);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * This scanner serves multiple purposes: <li>As normal index scan issued by
	 * client, the indexScannerContext is not null.</li> <li>As memstore scanner
	 * with deletes when flushing, indexScannerContext is null</li> <li>As index
	 * store files scanner for filter evaluation, indexScannerContext is null</li>
	 * <li>As store files only scanner for index rebuild, indexScannerContext is
	 * null</li>
	 * 
	 * @author danis
	 * 
	 */
	class IndexedRegionScanner implements RegionScanner {

		private final KeyProvider keyProvider;
		private KeyValue lastKeyValue;

		KeyValueHeap storeHeap = null;
		private final byte[] stopRow;
		private Filter filter;
		private List<KeyValue> results = new ArrayList<KeyValue>();
		private int batch;
		private int isScan;
		private boolean filterClosed = false;
		private long readPt;

		public HRegionInfo getRegionInfo() {
			return region.getRegionInfo();
		}

		IndexedRegionScanner(Scan scan,
				List<KeyValueScanner> additionalScanners,
				IndexScannerContext indexScannerContext) throws IOException {
			this.filter = scan.getFilter();
			this.batch = scan.getBatch();
			if (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
				this.stopRow = null;
			} else {
				this.stopRow = scan.getStopRow();
			}
			// If we are doing a get, we want to be [startRow,endRow] normally
			// it is [startRow,endRow) and if startRow=endRow we get nothing.
			this.isScan = scan.isGetScan() ? -1 : 0;

			// synchronize on scannerReadPoints so that nobody calculates
			// getSmallestReadPoint, before scannerReadPoints is updated.

			synchronized (scannerReadPoints) {
				this.readPt = MultiVersionConsistencyControl
						.resetThreadReadPoint(region.getMVCC());
				scannerReadPoints.put(this, this.readPt);
			}

			List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
			if (additionalScanners != null) {
				scanners.addAll(additionalScanners);
			}

			this.storeHeap = new KeyValueHeap(scanners, region.getRegionInfo()
					.getComparator());

			numberOfOngoingIndexedScans.incrementAndGet();
			totalIndexedScans.incrementAndGet();
			if (indexScannerContext != null) {
				keyProvider = new KeyProvider(indexScannerContext, scan);
			} else {
				keyProvider = null;
			}
		}

		/**
		 * Reset both the filter and the old filter.
		 */
		protected void resetFilters() {
			if (filter != null) {
				filter.reset();
			}
		}

		@Override
		public synchronized boolean next(List<KeyValue> outResults)
				throws IOException {
			// MultiVersionConsistencyControl.resetThreadReadPoint(region.getMVCC());
			seekNext();
			boolean result = next(outResults, batch);
			if (!outResults.isEmpty()) {
				lastKeyValue = outResults.get(0);
			}
			return result;
		}

		public synchronized boolean isFilterDone() {
			return this.filter != null && this.filter.filterAllRemaining();
		}

		@Override
		public synchronized boolean next(List<KeyValue> outResults, int limit)
				throws IOException {
			if (this.filterClosed) {
				throw new UnknownScannerException(
						"Scanner was closed (timed out?) "
								+ "after we renewed it. Could be caused by a very slow scanner "
								+ "or a lengthy garbage collection");
			}
			startRegionOperation();
			region.readRequestsCount.increment();
			try {

				// This could be a new thread from the last time we called
				// next().
				MultiVersionConsistencyControl.setThreadReadPoint(this.readPt);

				results.clear();

				boolean returnResult = nextInternal(limit);

				outResults.addAll(results);
				resetFilters();
				if (isFilterDone()) {
					return false;
				}
				return returnResult;
			} finally {
				closeRegionOperation();
			}
		}

		private boolean filterRow() {
			return filter != null && filter.filterRow();
		}

		private boolean filterRowKey(byte[] row) {
			return filter != null && filter.filterRowKey(row, 0, row.length);
		}

		protected void nextRow(byte[] currentRow) throws IOException {
			seekNext();
			while (Bytes.equals(currentRow, peekRow())) {
				this.storeHeap.next(MOCKED_LIST);
			}
			results.clear();
			resetFilters();
		}

		private byte[] peekRow() {
			KeyValue kv = this.storeHeap.peek();
			return kv == null ? null : kv.getRow();
		}

		private boolean isStopRow(byte[] currentRow) {
			return currentRow == null
					|| (stopRow != null && region
							.getRegionInfo()
							.getComparator()
							.compareRows(stopRow, 0, stopRow.length,
									currentRow, 0, currentRow.length) <= isScan);
		}

		protected void seekNext() throws IOException {
			if (keyProvider == null)
				return;
			KeyValue keyValue;
			do {
				keyValue = keyProvider.next();
				if (keyValue == null) {
					storeHeap.close();
					return;
				} else if (lastKeyValue == null) {
					break;
				} else {
					int comparisonResult = region.getRegionInfo()
							.getComparator()
							.compareRows(keyValue, lastKeyValue);
					if (comparisonResult > 0) {
						break;
					}
				}
			} while (true);
			storeHeap.seek(keyValue);
		}

		@Override
		public synchronized void close() {
			numberOfOngoingIndexedScans.decrementAndGet();
			if (keyProvider != null)
				keyProvider.close();
			if (storeHeap != null) {
				storeHeap.close();
				storeHeap = null;
			}
			scannerReadPoints.remove(this);
			this.filterClosed = true;
		}

		private boolean nextInternal(int limit) throws IOException {
			while (true) {
				byte[] currentRow = peekRow();
				if (isStopRow(currentRow)) {
					if (filter != null && filter.hasFilterRow()) {
						filter.filterRow(results);
					}
					if (filter != null && filter.filterRow()) {
						results.clear();
					}

					return false;
				} else if (filterRowKey(currentRow)) {
					nextRow(currentRow);
				} else {
					byte[] nextRow;
					do {
						this.storeHeap.next(results, limit - results.size());
						if (limit > 0 && results.size() == limit) {
							if (this.filter != null && filter.hasFilterRow()) {
								throw new IncompatibleFilterException(
										"Filter with filterRow(List<KeyValue>) incompatible with scan with limit!");
							}
							return true; // we are expecting more yes, but also
							// limited to how many we can
							// return.
						}
					} while (Bytes.equals(currentRow, nextRow = peekRow()));

					final boolean stopRow = isStopRow(nextRow);

					// now that we have an entire row, lets process with a
					// filters:

					// first filter with the filterRow(List)
					if (filter != null && filter.hasFilterRow()) {
						filter.filterRow(results);
					}

					if (results.isEmpty() || filterRow()) {
						// this seems like a redundant step - we already
						// consumed the row
						// there're no left overs.
						// the reasons for calling this method are:
						// 1. reset the filters.
						// 2. provide a hook to fast forward the row (used by
						// subclasses)
						nextRow(currentRow);

						// This row was totally filtered out, if this is NOT the
						// last row,
						// we should continue on.

						if (!stopRow)
							continue;
					}
					return !stopRow;
				}
			}
		}
	}

	private static final List<KeyValue> MOCKED_LIST = new AbstractList<KeyValue>() {

		@Override
		public void add(int index, KeyValue element) {
			// do nothing
		}

		@Override
		public boolean addAll(int index, Collection<? extends KeyValue> c) {
			return false; // this list is never changed as a result of an
			// update
		}

		@Override
		public KeyValue get(int index) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int size() {
			return 0;
		}
	};

	class KeyProvider {
		private KeyValueHeap memstoreHeap;
		private final IndexScannerContext indexScannerContext;
		private KeyValue currentMemstoreKey = null;
		private KeyValue currentExpressionKey = null;
		private boolean exhausted = false;
		private byte[] startRow;
		private boolean isStartRowSatisfied;

		KeyProvider(IndexScannerContext indexScannerContext, Scan scan)
				throws IOException {
			this.indexScannerContext = indexScannerContext;
			startRow = scan.getStartRow();
			isStartRowSatisfied = startRow == null;
			memstoreHeap = initMemstoreHeap(scan);
		}

		private KeyValueHeap initMemstoreHeap(Scan scan) throws IOException {
			List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
			for (byte[] family : region.getTableDesc().getFamiliesKeys()) {
				Store store = region.getStores().get(family);
				scanners.addAll(getMemstoreScanners(store, scan.getStartRow()));
			}
			return new KeyValueHeap(scanners, region.getRegionInfo()
					.getComparator());
		}

		public KeyValue next() throws IOException {
			if (exhausted)
				return null;
			KeyValue currentKey;
			if (currentMemstoreKey == null) {
				currentMemstoreKey = nextMemstoreRow();
			}
			if (currentExpressionKey == null) {
				currentExpressionKey = nextExpressionRow();
			}
			if (currentMemstoreKey == null && currentExpressionKey == null) {
				exhausted = true;
				return null;
			} else if (currentMemstoreKey == null) {
				currentKey = currentExpressionKey;
				currentExpressionKey = null;
			} else if (currentExpressionKey == null) {
				currentKey = currentMemstoreKey;
				currentMemstoreKey = null;
			} else {
				int comparsionResult = region.getRegionInfo().getComparator()
						.compareRows(currentMemstoreKey, currentExpressionKey);
				if (comparsionResult == 0) {
					currentExpressionKey = null;
					currentKey = currentMemstoreKey;
				} else if (comparsionResult < 0) {
					currentKey = currentMemstoreKey;
					currentMemstoreKey = null;
				} else {
					currentKey = currentExpressionKey;
					currentExpressionKey = null;
				}
			}
			return currentKey;
		}

		private KeyValue nextExpressionRow() {
			KeyValue nextExpressionKey = indexScannerContext.getNextKey();
			while (nextExpressionKey != null) {
				if (!isStartRowSatisfied) {
					if (region.getRegionInfo().getComparator()
							.compareRows(nextExpressionKey, startRow) >= 0) {
						isStartRowSatisfied = true;
						break;
					}
				} else {
					break;
				}
				nextExpressionKey = indexScannerContext.getNextKey();
			}
			return nextExpressionKey;
		}

		private KeyValue nextMemstoreRow() throws IOException {
			final KeyValue firstOnNextRow = this.memstoreHeap.next();
			KeyValue nextKeyValue = this.memstoreHeap.peek();
			while (firstOnNextRow != null
					&& nextKeyValue != null
					&& region.getRegionInfo().getComparator()
							.compareRows(firstOnNextRow, nextKeyValue) == 0) {
				this.memstoreHeap.next();
				nextKeyValue = this.memstoreHeap.peek();
			}
			return firstOnNextRow;
		}

		public void close() {
			this.memstoreHeap.close();
		}
	}

	@Override
	public String toString() {
		return region.toString();
	}

	@Override
	public String getRegionNameAsString() {
		return region.getRegionNameAsString();
	}

	@Override
	public Store getStore(byte[] family) {
		return region.getStore(family);
	}

	@Override
	public Configuration getConf() {
		return region.getConf();
	}

	@Override
	public boolean isClosed() {
		return region.isClosed();
	}

	@Override
	public boolean isClosing() {
		return region.isClosing();
	}

	@Override
	public MultiVersionConsistencyControl getMVCC() {
		return region.getMVCC();
	}

	@Override
	public HTableDescriptor getTableDesc() {
		return region.getTableDesc();
	}

	@Override
	public HRegionInfo getRegionInfo() {
		return region.regionInfo;
	}

	@Override
	public Path getTmpDir() {
		return region.getTmpDir();
	}

	@Override
	public FileSystem getFilesystem() {
		return region.getFilesystem();
	}

	@Override
	public RegionScanner getScanner(Scan scan) throws IOException {
		return region.getScanner(scan);
	}

	@Override
	public byte[] checkSplit() {
		return region.checkSplit();
	}

	@Override
	public Path getTableDir() {
		return region.tableDir;
	}
}
