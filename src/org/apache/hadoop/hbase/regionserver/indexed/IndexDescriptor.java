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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.VersionedWritable;

/**
 * The description of an indexed column family qualifier.
 * <p>
 * The description is composed of the following properties:
 * <ol>
 * <li>The qualifier name - specified which qualifier to index. The values
 * stored to this qualifier will serve as index keys.
 * <li>The qualifier type - type information for the qualifier. The type
 * information allows for custom ordering of index keys (which are qualifier
 * values) which may come handy when range queries are executed.
 * <li>offset - combine this property with the length property to allow partial
 * value extraction. Useful for keeping the index size small while for
 * qualifiers with large values. the offset specifies the starting point in the
 * value from which to extract the index key
 * <li>length - see also offset's description, the length property allows to
 * limit the number of bytes extracted to serve as index keys. If the bytes are
 * random a length of 1 or 2 bytes would yield very good results.
 * </ol>
 * </p>
 */
public class IndexDescriptor extends VersionedWritable {

	private static final byte VERSION = 1;

	/**
	 * Qualifier name;
	 */
	private byte[] qualifierName;

	/**
	 * Where to grab the column qualifier's value from. The default is from its
	 * first byte.
	 */
	private int offset = 0;

	/**
	 * Up-to where to grab the column qualifier's value. The default is all of
	 * it. A positive number would indicate a set limit.
	 */
	private int length = -1;

	/**
	 * Empty constructor to support the writable interface - DO NOT USE.
	 */
	public IndexDescriptor() {
	}

	/**
	 * Construct a new index descriptor.
	 * 
	 * @param qualifierName
	 *            the qualifier name
	 * @param indexType
	 *            the qualifier type
	 */
	public IndexDescriptor(byte[] qualifierName) {
		this.qualifierName = qualifierName;
	}

	/**
	 * The column family qualifier name.
	 * 
	 * @return column family qualifier name
	 */
	public byte[] getQualifierName() {
		return qualifierName;
	}

	/**
	 * The column family qualifier name.
	 * 
	 * @param qualifierName
	 *            column family qualifier name
	 */
	public void setQualifierName(byte[] qualifierName) {
		this.qualifierName = qualifierName;
	}

	/**
	 * The offset from which to extract the values.
	 * 
	 * @return the current offset value.
	 */
	public int getOffset() {
		return offset;
	}

	/**
	 * Sets the offset
	 * 
	 * @param offset
	 *            the offset from which to extract the values.
	 */
	public void setOffset(int offset) {
		this.offset = offset;
	}

	/**
	 * The length of the block extracted from the qualifier's value.
	 * 
	 * @return the length of the extracted value
	 */
	public int getLength() {
		return length;
	}

	/**
	 * The length of the extracted value.
	 * 
	 * @param length
	 *            the length of the extracted value.
	 */
	public void setLength(int length) {
		this.length = length;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		super.write(dataOutput);
		Bytes.writeByteArray(dataOutput, qualifierName);
		dataOutput.writeInt(offset);
		dataOutput.writeInt(length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		super.readFields(dataInput);
		qualifierName = Bytes.readByteArray(dataInput);
		this.offset = dataInput.readInt();
		this.length = dataInput.readInt();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public byte getVersion() {
		return VERSION;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		IndexDescriptor that = (IndexDescriptor) o;

		if (!Arrays.equals(qualifierName, that.qualifierName))
			return false;

		if (this.offset != that.offset)
			return false;

		if (this.length != that.length)
			return false;

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return Arrays.hashCode(qualifierName);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuffer s = new StringBuffer();
		s.append('{');
		s.append("QUALIFIER");
		s.append(" => '");
		s.append(Bytes.toString(qualifierName));
		s.append("'}");
		return s.toString();
	}
}
