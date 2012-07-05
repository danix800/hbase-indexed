package org.apache.hadoop.hbase.regionserver.indexed;

import java.util.Arrays;

public class Pair extends org.apache.hadoop.hbase.util.Pair<byte[], byte[]> {

	private static final long serialVersionUID = 1L;

	public Pair(byte[] first, byte[] second) {
		super(first, second);
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof Pair && equals(first, ((Pair) other).first)
				&& equals(second, ((Pair) other).second);
	}

	private static boolean equals(byte[] x, byte[] y) {
		return Arrays.equals(x, y);
	}

	@Override
	public int hashCode() {
		int hashCode = first != null ? generateHashCode(first) : 0;
		hashCode = 31 * hashCode + (second != null ? generateHashCode(second) : 0);
		return hashCode;
	}

	private int generateHashCode(byte[] o) {
		return Arrays.hashCode(o);
	}
}
