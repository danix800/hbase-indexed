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
package org.apache.hadoop.hbase.regionserver.indexed;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.IndexedHRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;

public class IndexRegion extends BaseEndpointCoprocessor implements
		IndexRegionProtocol {

	@Override
	public void index(boolean forceRebuild) throws IOException {
		RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment) getEnvironment();
		IndexedHRegion region = new IndexedHRegion(rce.getRegion());
		IndexManager indexManager = new IndexManagerImpl();
		indexManager.initialize(region);
		indexManager.reconstructIndexDescriptors();

		TreeSet<byte[]> indexSet = new TreeSet<byte[]>(new ByteArrayComparator());
		for (Pair pair : indexManager.getIndexDescriptors()) {
			indexSet.add(pair.getFirst());
		}
		FileSystem fs = region.getFilesystem();
		for (byte[] family : indexSet) {
			Store store = region.getStore(family);
			Path indexDir = new Path(store.getHomedir(), IndexManagerImpl.INDEX_DIR);
			if (forceRebuild) {
				if (fs.exists(indexDir) && !fs.delete(indexDir, true)) {
					throw new IOException("can't delete index dir: " + indexDir);
				}
			} else {
				if (fs.exists(indexDir)) {
					FileStatus[] files = fs.listStatus(indexDir);
					for (FileStatus file : files) {
						byte[] col = Bytes.toBytes(file.getPath().getName());
						indexManager.getIndexDescriptors().remove(new Pair(family, col));
					}
				}
			}
		}
		indexManager.buildIndexForAllStores();
	}

	private class ByteArrayComparator implements Comparator<byte[]> {

		@Override
		public int compare(byte[] o1, byte[] o2) {
			if (o1 == o2 || (o1 == null && o2 == null)) {
				return 0;
			}
			if (o1 == null && o2 != null) {
				return -1;
			}
			if (o1 != null && o2 == null) {
				return 1;
			}
			for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
				if (o1[i] != o2[i]) {
					return o1[i] - o2[i];
				}
			}
			return o1.length - o2.length;
		}
	}
}
