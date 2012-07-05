package org.apache.hadoop.hbase.regionserver.indexed;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.IndexedHRegion;
import org.apache.hadoop.hbase.regionserver.Store;

/**
 * Manages indexes
 * 
 * @author danis
 * 
 */
public abstract class IndexManager {

	/**
	 * Must be initialized before normal processes.
	 * 
	 * @param region
	 *          Wrapped HRegion
	 */
	public abstract void initialize(IndexedHRegion region);

	public abstract void reconstructIndexDescriptors() throws IOException;

	public abstract Set<Pair> getIndexDescriptors();

	/**
	 * Construct indexes from index store files.
	 * 
	 * @throws IOException
	 */
	public abstract void loadIndexFiles() throws IOException;

	public abstract void buildIndexForAllStores() throws IOException;

	public abstract void buildIndexForMemstore() throws IOException;

	/**
	 * If index scan is turned on and appropriate filters are passed then we
	 * evaluate those filters and return those matched rows as a scanner context.
	 * 
	 * @param scan
	 *          A client {@link Scan}
	 * @return {@link IndexScannerContext} Scanner context contains matched rows.
	 * @throws IOException
	 */
	public abstract IndexScannerContext createIndexScannerContext(Scan scan)
			throws IOException;

	/**
	 * Major compact those index store files.
	 * 
	 * @param store
	 *          The store we are compacting.
	 * @throws IOException
	 */
	public abstract void compactIndexStoreFiles(Store store) throws IOException;

	public abstract void rebuildIndexForWALRestore(List<KeyValue> kvs)
			throws IOException;

	public abstract void splitIndexFiles(byte[] splitRow) throws IOException;
}
