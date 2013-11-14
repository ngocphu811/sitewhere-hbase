/*
 * UuidCounterMap.java 
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
import java.util.UUID;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import com.sitewhere.hbase.HBaseConnectivity;
import com.sitewhere.hbase.ISiteWhereHBase;
import com.sitewhere.spi.SiteWhereException;

/**
 * Unique id mapper that generates UUIDs as keys and matches them to integer values.
 * 
 * @author Derek
 */
public class UnqiueIdCounterMap extends UniqueIdMap<String, Long> {

	public UnqiueIdCounterMap(HBaseConnectivity hbase, UniqueIdType keyIndicator, UniqueIdType valueIndicator) {
		super(hbase, keyIndicator, valueIndicator);
	}

	/**
	 * Create a UUID and add it to the UID table with corresponding numeric value.
	 * 
	 * @return
	 * @throws SiteWhereException
	 */
	public String createUniqueId() throws SiteWhereException {
		String uuid = UUID.randomUUID().toString();
		Long value = getNextCounterValue();
		create(uuid, value);
		return uuid;
	}

	/**
	 * Uses a counter row to keep unique values for the given key indicator type.
	 * 
	 * @return
	 * @throws SiteWhereException
	 */
	public Long getNextCounterValue() throws SiteWhereException {
		ByteBuffer counterRow = ByteBuffer.allocate(2);
		counterRow.put(UniqueIdType.CounterPlaceholder.getIndicator());
		counterRow.put(getKeyIndicator().getIndicator());
		GetRequest get = new GetRequest(ISiteWhereHBase.UID_TABLE_NAME, counterRow.array());
		try {
			// Check whether a counter row exists.
			ArrayList<KeyValue> results = getHbase().getClient().get(get).joinUninterruptibly();
			if (!results.isEmpty()) {
				// Increment existing counter row atomically.
				AtomicIncrementRequest request = new AtomicIncrementRequest(
						ISiteWhereHBase.UID_TABLE_NAME, counterRow.array(),
						ISiteWhereHBase.FAMILY_ID, UniqueIdMap.VALUE_QUAL);
				return getHbase().getClient().atomicIncrement(request).joinUninterruptibly();
			} else {
				// Write initial counter row.
				KeyValue one = new KeyValue(counterRow.array(), ISiteWhereHBase.FAMILY_ID,
						UniqueIdMap.VALUE_QUAL, Bytes.fromLong(1));
				PutRequest put = new PutRequest(ISiteWhereHBase.UID_TABLE_NAME, one);
				getHbase().getClient().put(put).joinUninterruptibly();
				return 1L;
			}
		} catch (Exception e) {
			throw new SiteWhereException("Unable to get next counter value.", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sitewhere.hbase.uid.UniqueIdMap#convertName(byte[])
	 */
	public String convertName(byte[] bytes) {
		return new String(bytes);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sitewhere.hbase.uid.UniqueIdMap#convertName(java.lang.Object)
	 */
	public byte[] convertName(String name) {
		return name.getBytes();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sitewhere.hbase.uid.UniqueIdMap#convertValue(byte[])
	 */
	public Long convertValue(byte[] bytes) {
		return Bytes.getLong(bytes);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sitewhere.hbase.uid.UniqueIdMap#convertValue(java.lang.Object)
	 */
	public byte[] convertValue(Long value) {
		return Bytes.fromLong(value);
	}
}