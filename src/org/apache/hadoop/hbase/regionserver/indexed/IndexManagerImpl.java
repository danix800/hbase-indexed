package org.apache.hadoop.hbase.regionserver.indexed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.IndexedHRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * IndexManager implementation.
 * 
 * @author danis
 * 
 */
public class IndexManagerImpl extends IndexManager {

	private static final Logger LOG = Logger.getLogger(IndexManagerImpl.class);
	public static String INDEX_DIR = "__INDEX__";

	private IndexedHRegion region;
	private IndexFilterEvaluator filterEvaluator;

	/**
	 * Index column to storefiles map, reconstructed from HColumnDescriptors.
	 */
	private Map<Pair, List<StoreFile>> indexMap;
	private Set<Pair> indexDescriptors;

	public IndexManagerImpl() {
		indexMap = new HashMap<Pair, List<StoreFile>>();
		indexDescriptors = new HashSet<Pair>();
	}

	@Override
	public void initialize(IndexedHRegion region) {
		this.region = region;
		filterEvaluator = new IndexFilterEvaluator(region);
	}

	@Override
	public Set<Pair> getIndexDescriptors() {
		return indexDescriptors;
	}

	@Override
	public void reconstructIndexDescriptors() throws IOException {
		indexDescriptors = buildIndexDescriptors();
	}

	private Set<Pair> buildIndexDescriptors() throws IOException {
		Set<Pair> indexDescriptors = new HashSet<Pair>();
		for (HColumnDescriptor columnDescriptor : region.getTableDesc()
				.getColumnFamilies()) {
			byte[] family = columnDescriptor.getName();
			for (IndexDescriptor indexDescriptor : IndexedTableUtils
					.getIndexDescriptors(columnDescriptor).values()) {
				indexDescriptors.add(new Pair(family, indexDescriptor
						.getQualifierName()));
			}
		}
		return indexDescriptors;
	}

	@Override
	public void loadIndexFiles() throws IOException {
		for (Pair indexDesc : indexDescriptors) {
			Store store = region.getStore(indexDesc.getFirst());
			Path indexDir = new Path(store.getHomedir(), INDEX_DIR);
			rebuildIfNoIndexDir(indexDir);
			List<StoreFile> files = indexMap.get(indexDesc);
			if (files == null) {
				files = new ArrayList<StoreFile>();
				indexMap.put(indexDesc, files);
			}
			files.addAll(getIndexStoreFiles(indexDir, store, indexDesc.getSecond()));
		}
		HRegionInfo hri = region.getRegionInfo();
		HRegionInfo hriWithoutId = new HRegionInfo(hri.getTableName(),
				hri.getStartKey(), hri.getEndKey(), false, 0);
		Path tableTmp = new Path(region.getTableDir(), ".tmp");
		Path hriWithoutIdPath = new Path(tableTmp, hriWithoutId.getEncodedName());
		FileSystem fs = region.getFilesystem();
		if (fs.exists(hriWithoutIdPath)) {
			fs.delete(hriWithoutIdPath, true);
		}
	}

	private void rebuildIfNoIndexDir(Path indexDir) throws IOException {
		FileSystem fs = region.getFilesystem();
		if (!fs.exists(indexDir)) {
			// InternalScanner scanner = region.getScanner(IndexedHRegion
			// .createInteranlFilesOnlyScan(indexDescriptors));
			// buildIndex(scanner);
			HRegionInfo hri = region.getRegionInfo();
			HRegionInfo hriWithoutId = new HRegionInfo(hri.getTableName(),
					hri.getStartKey(), hri.getEndKey(), false, 0);
			Path tableTmp = new Path(region.getTableDir(), ".tmp");
			Path hriWithoutIdPath = new Path(tableTmp, hriWithoutId.getEncodedName());
			if (fs.exists(hriWithoutIdPath)) {
				for (Pair indexDesc : indexDescriptors) {
					Store store = region.getStore(indexDesc.getFirst());
					Path tmpIndexDir = new Path(hriWithoutIdPath, store.getHomedir()
							.getName() + "/" + INDEX_DIR);
					if (fs.exists(tmpIndexDir)) {
						fs.rename(tmpIndexDir, indexDir);
					} else {
						// It's sad... try to rebuild
					}
				}
			}
		}
	}

	/**
	 * Store files are stored in tablename/region/store/index/qualifier
	 * 
	 * @param indexDir
	 *          tablename/region/store/index
	 * @param store
	 * @param qualifier
	 * @return
	 * @throws IOException
	 */
	private List<StoreFile> getIndexStoreFiles(Path indexDir, Store store,
			byte[] qualifier) throws IOException {
		List<StoreFile> results = new ArrayList<StoreFile>();
		Path indexColDir = new Path(indexDir, Bytes.toString(qualifier));
		FileSystem fs = region.getFilesystem();
		FileStatus files[] = fs.listStatus(indexColDir);
		for (int i = 0; files != null && i < files.length; i++) {
			if (files[i].isDir()) {
				continue;
			}
			Path p = files[i].getPath();
			if (fs.getFileStatus(p).getLen() <= 0) {
				LOG.warn("Skipping " + p + " because its empty. HBASE-646 DATA LOSS?");
				continue;
			}
			StoreFile curfile = IndexedHRegion.createStoreFile(fs, p,
					region.getConf(), store.getCacheConfig(), StoreFile.BloomType.NONE);
			curfile.createReader();
			if (LOG.isDebugEnabled()) {
				LOG.debug("loaded " + curfile.toStringDetailed());
			}
			results.add(curfile);
		}
		region.sortStoreFiles(results);
		return results;
	}

	@Override
	public void buildIndexForAllStores() throws IOException {
		if (indexDescriptors.size() <= 0) {
			return;
		}
		Scan scan = IndexedHRegion.createInteranlFilesOnlyScan(indexDescriptors);
		InternalScanner scanner = region.getScanner(scan);
		buildIndex(scanner);
	}

	@Override
	public void buildIndexForMemstore() throws IOException {
		if (indexDescriptors.size() <= 0) {
			return;
		}
		InternalScanner scanner = region
				.getMemstoreScannerWithDeletes(createScan(indexDescriptors));
		Map<Pair, StoreFile> sfs = buildIndex(scanner);
		for (Map.Entry<Pair, StoreFile> entry : sfs.entrySet()) {
			Pair indexDesc = entry.getKey();
			StoreFile sf = entry.getValue();
			List<StoreFile> files = indexMap.get(indexDesc);
			if (files == null) {
				files = new ArrayList<StoreFile>();
				indexMap.put(indexDesc, files);
			}
			files.add(sf);
			region.sortStoreFiles(files);
		}
	}

	private Map<Pair, StoreFile> buildIndex(InternalScanner scanner)
			throws IOException {
		Map<Pair, TreeSet<KeyValue>> newValueMap = new HashMap<Pair, TreeSet<KeyValue>>();
		Map<Pair, StoreFile> results = new HashMap<Pair, StoreFile>();
		FileSystem fs = region.getFilesystem();
		try {
			boolean moreRows;
			int id = 0;
			do {
				List<KeyValue> nextRow = new ArrayList<KeyValue>();
				moreRows = scanner.next(nextRow);
				Store store = null;
				if (nextRow.size() > 0) {
					byte[] family = nextRow.get(0).getFamily();
					if (store == null) {
						store = region.getStore(family);
					}
					for (KeyValue keyValue : nextRow) {
						try {
							Pair indexDes = new Pair(family, keyValue.getQualifier());
							TreeSet<KeyValue> values = newValueMap.get(indexDes);
							if (values == null) {
								values = new TreeSet<KeyValue>(region.getRegionInfo()
										.getComparator());
								newValueMap.put(indexDes, values);
							}
							byte[] key = IndexedTableUtils.extractKey(keyValue);
							KeyValue kv = new KeyValue(key, Bytes.toBytes("rows"),
									keyValue.getRow(), keyValue.getTimestamp(),
									KeyValue.Type.codeToType(keyValue.getType()),
									keyValue.getRow());
							values.add(kv);
						} catch (Exception e) {
							LOG.error("Failed to add " + keyValue + " to the index", e);
						}
					}
				}
				id++;
				if (id % 10000 == 0 && LOG.isDebugEnabled()) {
					LOG.debug("indexing " + region.getRegionNameAsString() + ": " + id
							+ " rows");
				}
				nextRow.clear();
			} while (moreRows);
		} finally {
			scanner.close();
			for (Map.Entry<Pair, TreeSet<KeyValue>> entry : newValueMap.entrySet()) {
				Pair indexDes = entry.getKey();
				byte[] family = indexDes.getFirst();
				byte[] col = indexDes.getSecond();
				Store store = region.getStore(family);
				TreeSet<KeyValue> values = entry.getValue();
				Writer writer = StoreFile.createWriter(fs, region.getTmpDir(),
						HFile.DEFAULT_BLOCKSIZE, region.getConf(), store.getCacheConfig());
				for (KeyValue kv : values) {
					writer.append(kv);
				}
				writer.appendMetadata(0, false);
				writer.close();
				Path dstDir = new Path(store.getHomedir(), INDEX_DIR + "/"
						+ Bytes.toString(col));
				if (!fs.exists(dstDir)) {
					fs.mkdirs(dstDir);
				}
				Path dstPath = new Path(dstDir, writer.getPath().getName());
				if (!fs.rename(writer.getPath(), dstPath)) {
					LOG.error("rename failed: " + writer.getPath() + " => " + dstPath);
				}
				StoreFile sf = IndexedHRegion.createStoreFile(fs, dstPath,
						region.getConf(), store.getCacheConfig(), StoreFile.BloomType.NONE);
				sf.createReader();
				results.put(indexDes, sf);
			}
		}
		return results;
	}

	private Scan createScan(Set<Pair> columns) {
		Scan scan = new Scan();
		for (Pair column : columns) {
			scan.addColumn(column.getFirst(), column.getSecond());
		}
		return scan;
	}

	@Override
	public IndexScannerContext createIndexScannerContext(Scan scan)
			throws IOException {
		if (!IndexedTableUtils.isIndexedScan(scan)) {
			return null;
		}
		List<KeyValue> matchedExpression = filterEvaluator.evaluate(indexMap,
				scan.getFilter());
		Collections.sort(matchedExpression, region.getRegionInfo().getComparator());
		return matchedExpression != null ? new IndexScannerContext(
				matchedExpression) : null;
	}

	@Override
	public void compactIndexStoreFiles(Store store) throws IOException {
		byte[] family = store.getFamily().getName();
		for (Pair famCol : indexDescriptors) {
			if (Bytes.compareTo(family, famCol.getFirst()) == 0) {
				byte[] col = famCol.getSecond();
				List<StoreFile> sfs = indexMap.get(famCol);
				if (sfs == null || sfs.size() < 3) {
					continue;
				}
				StoreFile sf = region.compactIndexFiles(sfs, store, col);
				for (StoreFile file : sfs) {
					file.deleteReader();
				}
				sfs.clear();
				sfs.add(sf);
			}
		}
	}

	@Override
	public void rebuildIndexForWALRestore(List<KeyValue> kvs) throws IOException {
		reconstructIndexDescriptors();
		if(indexDescriptors.size() <= 0) {
			return;
		}
		buildIndex(region.createCollectionBackedScanner(kvs));
	}

	@Override
	public void splitIndexFiles(byte[] splitRow) throws IOException {
		Path tableTmp = new Path(region.getTableDir(), ".tmp");
		HRegionInfo hri = region.getRegionInfo();
		byte[] startKey = hri.getStartKey();
		byte[] endKey = hri.getEndKey();

		HRegionInfo hri_a = new HRegionInfo(hri.getTableName(), startKey, splitRow,
				false, 0);
		HRegionInfo hri_b = new HRegionInfo(hri.getTableName(), splitRow, endKey,
				false, 0);

		Path hri_aPath = new Path(tableTmp, hri_a.getEncodedName());
		Path hri_bPath = new Path(tableTmp, hri_b.getEncodedName());
		FileSystem fs = region.getFilesystem();

		if (!fs.exists(hri_aPath)) {
			fs.mkdirs(hri_aPath);
		}
		if (!fs.exists(hri_bPath)) {
			fs.mkdirs(hri_bPath);
		}

		for (Map.Entry<Pair, List<StoreFile>> entry : indexMap.entrySet()) {
			Pair indexDesc = entry.getKey();
			byte[] family = indexDesc.getFirst();
			byte[] col = indexDesc.getSecond();
			Path hri_aColPath = new Path(hri_aPath, Bytes.toString(family) + "/"
					+ INDEX_DIR + "/" + Bytes.toString(col));
			Path hri_bColPath = new Path(hri_bPath, Bytes.toString(family) + "/"
					+ INDEX_DIR + "/" + Bytes.toString(col));
			if (!fs.exists(hri_aColPath)) {
				fs.mkdirs(hri_aColPath);
			}
			if (!fs.exists(hri_bColPath)) {
				fs.mkdirs(hri_bColPath);
			}
			List<StoreFile> files = entry.getValue();
			splitIndexFile(fs, indexDesc, files, splitRow, hri_aColPath,
					hri_bColPath, region.getStore(family).getCacheConfig());
		}
	}

	private void splitIndexFile(FileSystem fs, Pair indexDesc,
			List<StoreFile> files, byte[] splitRow, Path hri_aColPath,
			Path hri_bColPath, CacheConfig config) throws IOException {

		TreeSet<KeyValue> newValue_a = new TreeSet<KeyValue>(region.getRegionInfo()
				.getComparator());
		TreeSet<KeyValue> newValue_b = new TreeSet<KeyValue>(region.getRegionInfo()
				.getComparator());
		InternalScanner scanner = region.instantiateIndexStoreFileScanner(files,
				new Scan());

		try {
			List<KeyValue> nextRows = new ArrayList<KeyValue>();
			boolean moreRows;
			do {
				moreRows = scanner.next(nextRows);
				if (nextRows.size() > 0) {
					KeyValue splitValue = new KeyValue(nextRows.get(0).getRow(),
							Bytes.toBytes("rows"), splitRow, 0, KeyValue.Type.Delete,
							splitRow);
					int index = Collections.binarySearch(nextRows, splitValue,
							new Comparator<KeyValue>() {
								@Override
								public int compare(KeyValue o1, KeyValue o2) {
									return Bytes.compareTo(o1.getValue(), o2.getValue());
								}
							});
					if (index < 0) {
						index = -index - 1;
					}
					for (int i = 0; i < nextRows.size(); i++) {
						if (i < index) {
							newValue_a.add(nextRows.get(i));
						} else {
							newValue_b.add(nextRows.get(i));
						}
					}
				}
				nextRows.clear();
			} while (moreRows);
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			Writer writer = StoreFile.createWriter(fs, hri_aColPath,
					HFile.DEFAULT_BLOCKSIZE, region.getConf(), config);
			for (KeyValue kv : newValue_a) {
				writer.append(kv);
			}
			writer.appendMetadata(0, false);
			writer.close();

			writer = StoreFile.createWriter(fs, hri_bColPath,
					HFile.DEFAULT_BLOCKSIZE, region.getConf(), config);
			for (KeyValue kv : newValue_b) {
				writer.append(kv);
			}
			writer.appendMetadata(0, false);
			writer.close();
		}
	}
}
