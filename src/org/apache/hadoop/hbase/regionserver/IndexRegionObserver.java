package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.indexed.IndexManager;
import org.apache.hadoop.hbase.regionserver.indexed.IndexManagerImpl;
import org.apache.hadoop.hbase.regionserver.indexed.IndexScannerContext;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Region-level indexing observer, by hooking to region lifecycle events, also
 * hook to scanner openning to switch to our index scanners. Delegates index
 * management to {@link IndexManager}. Memstores are not indexed until flushing
 * to store files.
 * 
 * @author danis
 * 
 */
public class IndexRegionObserver extends BaseRegionObserver {

	/**
	 * Index management.
	 */
	private IndexManager indexManager;
	/**
	 * Wrapped HRegion.
	 */
	private IndexedHRegion region;

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment) e;
		this.region = new IndexedHRegion(rce.getRegion());
		indexManager = new IndexManagerImpl();
		indexManager.initialize(region);
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		// indexManager.cleanup();
	}

	/**
	 * After region opened, we construct index from store files according to index
	 * descriptors inside HColumnDescriptor.
	 */
	@Override
	public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
		try {
			// Construct indexes from store files.
			indexManager.reconstructIndexDescriptors();
			indexManager.loadIndexFiles();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * It's time to flush memstores.
	 */
	@Override
	public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) {
		try {
			MultiVersionConsistencyControl.resetThreadReadPoint(region.getMVCC());
			indexManager.buildIndexForMemstore();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	@Override
	public void postWALRestore(ObserverContext<RegionCoprocessorEnvironment> env,
			HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
		try {
			List<KeyValue> kvs = new ArrayList<KeyValue>();
			for (KeyValue kv : logEdit.getKeyValues()) {
				if (kv.matchingFamily(HLog.METAFAMILY)
						|| !Bytes.equals(logKey.getEncodedRegionName(), region
								.getRegionInfo().getEncodedNameAsBytes())) {
					continue;
				}
				kvs.add(kv);
			}
			if (kvs.size() > 0) {
				indexManager.rebuildIndexForWALRestore(kvs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Using our own RegionScanner, if index scan is turned on and we have
	 * appropriate filters.
	 */
	@Override
	public RegionScanner preScannerOpen(
			ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,
			RegionScanner s) throws IOException {
		try {
			IndexScannerContext indexScannerContext = indexManager
					.createIndexScannerContext(scan);
			/*
			 * We can use index scan so bypass the normal scanner open by instantiate
			 * our own scanner.
			 */
			if (indexScannerContext != null) {
				e.bypass();
				return region.instantiateRegionScanner(scan, indexScannerContext);
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		return null;
	}

	/**
	 * After a few flushes we may end up with lots of index store files, so we
	 * need to compact those files just like normal compactions in HBase. We have
	 * only major compactions now, and the selection is less strict.
	 */
	@Override
	public InternalScanner preCompact(
			ObserverContext<RegionCoprocessorEnvironment> e, Store store,
			InternalScanner scanner) {
		try {
			indexManager.compactIndexStoreFiles(store);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return super.preCompact(e, store, scanner);
	}

	@Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) {
		try {
			byte[] splitRow = region.checkSplit();
			indexManager.splitIndexFiles(splitRow);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
