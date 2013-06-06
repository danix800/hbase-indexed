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
		hashCode = 31 * hashCode
				+ (second != null ? generateHashCode(second) : 0);
		return hashCode;
	}

	private int generateHashCode(byte[] o) {
		return Arrays.hashCode(o);
	}
}
