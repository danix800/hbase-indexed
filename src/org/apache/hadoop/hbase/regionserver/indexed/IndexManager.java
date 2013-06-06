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
	 *            Wrapped HRegion
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
	 * evaluate those filters and return those matched rows as a scanner
	 * context.
	 * 
	 * @param scan
	 *            A client {@link Scan}
	 * @return {@link IndexScannerContext} Scanner context contains matched
	 *         rows.
	 * @throws IOException
	 */
	public abstract IndexScannerContext createIndexScannerContext(Scan scan)
			throws IOException;

	/**
	 * Major compact those index store files.
	 * 
	 * @param store
	 *            The store we are compacting.
	 * @throws IOException
	 */
	public abstract void compactIndexStoreFiles(Store store) throws IOException;

	public abstract void rebuildIndexForWALRestore(List<KeyValue> kvs)
			throws IOException;

	public abstract void splitIndexFiles(byte[] splitRow) throws IOException;
}
