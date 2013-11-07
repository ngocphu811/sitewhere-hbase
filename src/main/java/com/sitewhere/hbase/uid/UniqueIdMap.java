/*
 * UniqueIdMap.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.hbase.uid;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

import com.sitewhere.hbase.HBaseConnectivity;
import com.sitewhere.hbase.SiteWhereHbaseConstants;
import com.sitewhere.spi.SiteWhereException;

/**
 * Handles conversions to/from a given type of unique id.
 * 
 * @author Derek
 */
public abstract class UniqueIdMap<N, V> {

	/** Static logger instance */
	private static Logger LOGGER = Logger.getLogger(UniqueIdMap.class);

	/** Qualifier for columns containing values */
	public static final byte[] VALUE_QUAL = Bytes.UTF8("value");

	/** HBase client */
	protected HBaseConnectivity hbase;

	/** Key type indicator */
	protected UniqueIdType keyIndicator;

	/** Value type indicator */
	protected UniqueIdType valueIndicator;

	/** Map of names to values */
	private Map<N, V> nameToValue = new HashMap<N, V>();

	/** Maps of values to names */
	private Map<V, N> valueToName = new HashMap<V, N>();

	public UniqueIdMap(HBaseConnectivity hbase, UniqueIdType keyIndicator, UniqueIdType valueIndicator) {
		this.hbase = hbase;
		this.keyIndicator = keyIndicator;
		this.valueIndicator = valueIndicator;
	}

	/**
	 * Create mapping and reverse mapping in UID table. Create value-to-name first, so if
	 * it fails we do not have names without reverse mappings.
	 * 
	 * @param name
	 * @param value
	 * @throws SiteWhereException
	 */
	public void create(N name, V value) throws SiteWhereException {
		createValueToName(value, name);
		createNameToValue(name, value);
	}

	/**
	 * Create name to value row in the UID table.
	 * 
	 * @param name
	 * @param value
	 * @throws SiteWhereException
	 */
	protected void createNameToValue(N name, V value) throws SiteWhereException {
		byte[] nameBytes = convertName(name);
		ByteBuffer nameBuffer = ByteBuffer.allocate(nameBytes.length + 1);
		nameBuffer.put(keyIndicator.getIndicator());
		nameBuffer.put(nameBytes);
		byte[] valueBytes = convertValue(value);
		PutRequest namePut = new PutRequest(SiteWhereHbaseConstants.UID_TABLE_NAME, nameBuffer.array(),
				SiteWhereHbaseConstants.FAMILY_ID, VALUE_QUAL, valueBytes);
		try {
			hbase.getClient().put(namePut).joinUninterruptibly();
			nameToValue.put(name, value);
		} catch (Exception e) {
			throw new SiteWhereException("Unable to store name mapping in UID table.", e);
		}
	}

	/**
	 * Create value to name row in the UID table.
	 * 
	 * @param name
	 * @param value
	 * @throws SiteWhereException
	 */
	protected void createValueToName(V value, N name) throws SiteWhereException {
		byte[] valueBytes = convertValue(value);
		ByteBuffer valueBuffer = ByteBuffer.allocate(valueBytes.length + 1);
		valueBuffer.put(valueIndicator.getIndicator());
		valueBuffer.put(valueBytes);
		byte[] nameBytes = convertName(name);
		PutRequest valuePut = new PutRequest(SiteWhereHbaseConstants.UID_TABLE_NAME, valueBuffer.array(),
				SiteWhereHbaseConstants.FAMILY_ID, VALUE_QUAL, nameBytes);
		try {
			hbase.getClient().put(valuePut).joinUninterruptibly();
			valueToName.put(value, name);
		} catch (Exception e) {
			throw new SiteWhereException("Unable to store value mapping in UID table.", e);
		}
	}

	/**
	 * Refresh from HBase UID table.
	 * 
	 * @throws SiteWhereException
	 */
	public void refresh() throws SiteWhereException {
		try {
			List<KeyValue> ntvList = getValuesForType(keyIndicator);
			for (KeyValue ntv : ntvList) {
				byte[] key = ntv.key();
				byte[] nameBytes = new byte[key.length - 1];
				System.arraycopy(key, 1, nameBytes, 0, nameBytes.length);
				N name = convertName(nameBytes);
				V value = convertValue(ntv.value());
				nameToValue.put(name, value);
			}
			List<KeyValue> vtnList = getValuesForType(valueIndicator);
			for (KeyValue vtn : vtnList) {
				byte[] key = vtn.key();
				byte[] valueBytes = new byte[key.length - 1];
				System.arraycopy(key, 1, valueBytes, 0, valueBytes.length);
				V value = convertValue(valueBytes);
				N name = convertName(vtn.value());
				valueToName.put(value, name);
			}
		} catch (Throwable t) {
			throw new SiteWhereException(t);
		}
	}

	/**
	 * Get all {@link KeyValue} results for the given uid type.
	 * 
	 * @param start
	 * @param end
	 * @return
	 * @throws Exception
	 */
	protected List<KeyValue> getValuesForType(UniqueIdType type) throws Exception {
		Scanner keyScanner = hbase.getClient().newScanner(SiteWhereHbaseConstants.UID_TABLE_NAME);
		byte startByte = keyIndicator.getIndicator();
		byte stopByte = keyIndicator.getIndicator();
		stopByte++;
		byte[] startKey = { startByte };
		byte[] stopKey = { stopByte };
		keyScanner.setStartKey(startKey);
		keyScanner.setStopKey(stopKey);
		List<KeyValue> results = new ArrayList<KeyValue>();
		ArrayList<ArrayList<KeyValue>> currentBatch;
		while ((currentBatch = keyScanner.nextRows().joinUninterruptibly()) != null) {
			for (ArrayList<KeyValue> row : currentBatch) {
				results.addAll(row);
			}
		}
		keyScanner.close();
		return results;
	}

	/**
	 * Get value based on name.
	 * 
	 * @param name
	 * @return
	 */
	public V getValue(N name) {
		V result = nameToValue.get(name);
		if (result == null) {
			result = getValueFromTable(name);
		}
		return result;
	}

	/**
	 * Get the current value for name from UID table.
	 * 
	 * @param name
	 * @return
	 */
	protected V getValueFromTable(N name) {
		byte[] nameBytes = convertName(name);
		ByteBuffer nameBuffer = ByteBuffer.allocate(nameBytes.length + 1);
		nameBuffer.put(keyIndicator.getIndicator());
		nameBuffer.put(nameBytes);
		GetRequest request = new GetRequest(SiteWhereHbaseConstants.UID_TABLE_NAME, nameBuffer.array());
		try {
			ArrayList<KeyValue> matches = hbase.getClient().get(request).joinUninterruptibly();
			if (matches.size() > 0) {
				byte[] value = matches.get(0).value();
				return convertValue(value);
			}
			return null;
		} catch (Exception e) {
			LOGGER.error("Error locating name to value mapping.", e);
			return null;
		}
	}

	/**
	 * Get name based on value.
	 * 
	 * @param value
	 * @return
	 */
	public N getName(V value) {
		N result = valueToName.get(value);
		if (result == null) {
			result = getNameFromTable(value);
		}
		return result;
	}

	/**
	 * Get the current name for value from UID table.
	 * 
	 * @param name
	 * @return
	 */
	protected N getNameFromTable(V value) {
		byte[] valueBytes = convertValue(value);
		ByteBuffer valueBuffer = ByteBuffer.allocate(valueBytes.length + 1);
		valueBuffer.put(valueIndicator.getIndicator());
		valueBuffer.put(valueBytes);
		GetRequest request = new GetRequest(SiteWhereHbaseConstants.UID_TABLE_NAME, valueBuffer.array());
		try {
			ArrayList<KeyValue> matches = hbase.getClient().get(request).joinUninterruptibly();
			if (matches.size() > 0) {
				byte[] name = matches.get(0).value();
				return convertName(name);
			}
			return null;
		} catch (Exception e) {
			LOGGER.error("Error locating value to name mapping.", e);
			return null;
		}
	}

	/** Used to convert stored name to correct datatype */
	public abstract N convertName(byte[] bytes);

	/** Used to convert stored name to correct datatype */
	public abstract byte[] convertName(N name);

	/** Used to convert stored value to correct datatype */
	public abstract V convertValue(byte[] bytes);

	/** Used to convert stored value to correct datatype */
	public abstract byte[] convertValue(V value);

	/** Get HBase connectivity accessor */
	public HBaseConnectivity getHbase() {
		return hbase;
	}

	/** Get indicator for key rows for this type */
	public UniqueIdType getKeyIndicator() {
		return keyIndicator;
	}

	/** Get indicator for value rows for this type */
	public UniqueIdType getValueIndicator() {
		return valueIndicator;
	}
}