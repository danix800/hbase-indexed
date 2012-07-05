package org.apache.hadoop.hbase.regionserver.indexed;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

public class IndexedTableUtils {

	public static final ImmutableBytesWritable INDEX_DESCRIPTORS = new ImmutableBytesWritable(
			Bytes.toBytes("INDEX_DESC"));
	public static final String INDEXED_SCAN = "INDEXED_SCAN";

	public static void addIndexDescriptor(HColumnDescriptor columnDescriptor,
			IndexDescriptor indexDescriptor) throws IOException {

		ImmutableBytesWritable qualifierName = new ImmutableBytesWritable(
				indexDescriptor.getQualifierName());
		Map<ImmutableBytesWritable, IndexDescriptor> indexDescriptorMap = getIndexDescriptors(columnDescriptor);
		if (indexDescriptorMap.containsKey(qualifierName)) {
			throw new IllegalArgumentException(
					"An index already exists on qualifier '"
							+ Bytes.toString(indexDescriptor.getQualifierName()) + "'");
		}
		indexDescriptorMap.put(qualifierName, indexDescriptor);
		setIndexDescriptors(columnDescriptor, indexDescriptorMap);
	}

	public static Map<ImmutableBytesWritable, IndexDescriptor> getIndexDescriptors(
			HColumnDescriptor columnDescriptor) throws IOException {
		Map<ImmutableBytesWritable, ImmutableBytesWritable> values = columnDescriptor
				.getValues();
		if (hasIndexDescriptors(columnDescriptor)) {
			DataInputBuffer in = new DataInputBuffer();
			byte[] bytes = values.get(INDEX_DESCRIPTORS).get();
			in.reset(bytes, bytes.length);

			int size = in.readInt();
			Map<ImmutableBytesWritable, IndexDescriptor> indexDescriptors = new HashMap<ImmutableBytesWritable, IndexDescriptor>(
					size);

			for (int i = 0; i < size; i++) {
				IndexDescriptor indexDescriptor = new IndexDescriptor();
				indexDescriptor.readFields(in);
				indexDescriptors.put(
						new ImmutableBytesWritable(indexDescriptor.getQualifierName()),
						indexDescriptor);
			}

			return indexDescriptors;
		} else {
			return new HashMap<ImmutableBytesWritable, IndexDescriptor>();
		}
	}

	public static boolean hasIndexDescriptors(HColumnDescriptor columnDescriptor) {
		return columnDescriptor.getValues().containsKey(INDEX_DESCRIPTORS);
	}

	public static void setIndexDescriptors(
			HColumnDescriptor indexedHColumnDescriptor,
			Map<ImmutableBytesWritable, IndexDescriptor> indexDescriptorMap)
			throws IOException {
		DataOutputBuffer out = new DataOutputBuffer();
		out.writeInt(indexDescriptorMap.size());
		for (IndexDescriptor indexDescriptor : indexDescriptorMap.values()) {
			indexDescriptor.write(out);
		}

		indexedHColumnDescriptor.setValue(INDEX_DESCRIPTORS.get(), out.getData());
	}

	public static boolean isIndexedScan(Scan scan) {
		byte[] b = scan.getAttribute(INDEXED_SCAN);
		return b != null ? Bytes.toBoolean(b) : false;
	}

	public static void setIndexedScan(Scan scan, boolean indexing) {
		scan.setAttribute(INDEXED_SCAN, Bytes.toBytes(indexing));
	}

	public static byte[] extractKey(KeyValue kv) {
		int valueLength = kv.getValueLength();
		int o = kv.getValueOffset();
		byte[] result = new byte[valueLength];
		System.arraycopy(kv.getBuffer(), o, result, 0, valueLength);
		return result;
	}

	public static byte[] incrementAtIndex(byte[] array, int index) {
		byte[] result = Arrays.copyOf(array, array.length);

		recurseincrement(result, index);
		return result;
	}

	private static void recurseincrement(byte[] array, int index) {
		if (array[index] == Byte.MAX_VALUE) {
			array[index] = 0;
			if (index > 0)
				incrementAtIndex(array, index - 1);
		} else {
			array[index]++;
		}
	}
}
