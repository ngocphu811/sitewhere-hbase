/*
 * HBaseSite.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.hbase.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sitewhere.core.device.SiteWherePersistence;
import com.sitewhere.hbase.HBaseConnectivity;
import com.sitewhere.hbase.SiteWhereHBaseConstants;
import com.sitewhere.hbase.common.MarshalUtils;
import com.sitewhere.hbase.uid.IdManager;
import com.sitewhere.rest.model.device.Site;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.SiteWhereSystemException;
import com.sitewhere.spi.device.ISite;
import com.sitewhere.spi.device.request.ISiteCreateRequest;
import com.sitewhere.spi.error.ErrorCode;
import com.sitewhere.spi.error.ErrorLevel;

/**
 * HBase specifics for dealing with SiteWhere sites.
 * 
 * @author Derek
 */
public class HBaseSite {

	/** Length of site identifier (subset of 8 byte long) */
	public static final int SITE_IDENTIFIER_LENGTH = 2;

	/** Column qualifier for zone counter */
	public static final byte[] ZONE_COUNTER = Bytes.UTF8("zonectr");

	/** Column qualifier for assignment counter */
	public static final byte[] ASSIGNMENT_COUNTER = Bytes.UTF8("assnctr");

	/**
	 * Create a new site.
	 * 
	 * @param hbase
	 * @param uids
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static ISite createSite(HBaseConnectivity hbase, ISiteCreateRequest request)
			throws SiteWhereException {
		String uuid = IdManager.getInstance().getSiteKeys().createUniqueId();
		Long value = IdManager.getInstance().getSiteKeys().getValue(uuid);
		byte[] primary = getPrimaryRowkey(value);

		// Use common logic so all backend implementations work the same.
		Site site = SiteWherePersistence.siteCreateLogic(request, uuid);

		// Create primary site record.
		byte[] json = MarshalUtils.marshalJson(site);
		byte[] maxLong = Bytes.fromLong(Long.MAX_VALUE);
		byte[][] qualifiers = { SiteWhereHBaseConstants.JSON_CONTENT, ZONE_COUNTER, ASSIGNMENT_COUNTER };
		byte[][] values = { json, maxLong, maxLong };
		PutRequest put = new PutRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, primary,
				SiteWhereHBaseConstants.FAMILY_ID, qualifiers, values);
		HBasePersistence.syncPut(hbase, put, "Unable to create site.");

		return site;
	}

	/**
	 * Get a site based on unique token.
	 * 
	 * @param hbase
	 * @param token
	 * @return
	 * @throws SiteWhereException
	 */
	public static Site getSiteByToken(HBaseConnectivity hbase, String token) throws SiteWhereException {
		Long siteId = IdManager.getInstance().getSiteKeys().getValue(token);
		if (siteId == null) {
			return null;
		}
		byte[] primary = getPrimaryRowkey(siteId);
		GetRequest request = new GetRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, primary).family(
				SiteWhereHBaseConstants.FAMILY_ID).qualifier(SiteWhereHBaseConstants.JSON_CONTENT);
		ArrayList<KeyValue> results = HBasePersistence.syncGet(hbase, request,
				"Unable to load site by token.");
		if (results.size() != 1) {
			throw new SiteWhereException("Expected one JSON entry for site and found: " + results.size());
		}
		byte[] json = results.get(0).value();
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(json, Site.class);
		} catch (Throwable e) {
			throw new SiteWhereException("Unable to parse site JSON.", e);
		}
	}

	/**
	 * Update information for an existing site.
	 * 
	 * @param hbase
	 * @param token
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static Site updateSite(HBaseConnectivity hbase, String token, ISiteCreateRequest request)
			throws SiteWhereException {
		Site updated = getSiteByToken(hbase, token);
		if (updated == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidSiteToken, ErrorLevel.ERROR);
		}

		// Use common update logic so that backend implemetations act the same way.
		SiteWherePersistence.siteUpdateLogic(request, updated);

		Long siteId = IdManager.getInstance().getSiteKeys().getValue(token);
		byte[] rowkey = getPrimaryRowkey(siteId);
		byte[] json = MarshalUtils.marshalJson(updated);
		PutRequest put = new PutRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, rowkey,
				SiteWhereHBaseConstants.FAMILY_ID, SiteWhereHBaseConstants.JSON_CONTENT, json);
		HBasePersistence.syncPut(hbase, put, "Unable to update site.");
		return updated;
	}

	/**
	 * Allocate the next zone id and return the new value. (Each id is less than the last)
	 * 
	 * @param hbase
	 * @param siteId
	 * @return
	 * @throws SiteWhereException
	 */
	public static Long allocateNextZoneId(HBaseConnectivity hbase, Long siteId) throws SiteWhereException {
		byte[] primary = getPrimaryRowkey(siteId);
		AtomicIncrementRequest request = new AtomicIncrementRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME,
				primary, SiteWhereHBaseConstants.FAMILY_ID, ZONE_COUNTER, -1);
		try {
			return hbase.getClient().atomicIncrement(request).joinUninterruptibly();
		} catch (Exception e) {
			throw new SiteWhereException("Unable to allocate next zone id.", e);
		}
	}

	/**
	 * Allocate the next assignment id and return the new value. (Each id is less than the
	 * last)
	 * 
	 * @param hbase
	 * @param siteId
	 * @return
	 * @throws SiteWhereException
	 */
	public static Long allocateNextAssignmentId(HBaseConnectivity hbase, Long siteId)
			throws SiteWhereException {
		byte[] primary = getPrimaryRowkey(siteId);
		AtomicIncrementRequest request = new AtomicIncrementRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME,
				primary, SiteWhereHBaseConstants.FAMILY_ID, ASSIGNMENT_COUNTER, -1);
		try {
			return hbase.getClient().atomicIncrement(request).joinUninterruptibly();
		} catch (Exception e) {
			throw new SiteWhereException("Unable to allocate next assignment id.", e);
		}
	}

	/**
	 * Get the unique site identifier based on the long value associated with the site
	 * UUID. This will be a subset of the full 8-bit long value.
	 * 
	 * @param value
	 * @return
	 */
	public static byte[] getSiteIdentifier(Long value) {
		byte[] bytes = Bytes.fromLong(value);
		byte[] result = new byte[SITE_IDENTIFIER_LENGTH];
		System.arraycopy(bytes, bytes.length - SITE_IDENTIFIER_LENGTH, result, 0, SITE_IDENTIFIER_LENGTH);
		return result;
	}

	/**
	 * Get primary row key for a given site.
	 * 
	 * @param siteId
	 * @return
	 */
	public static byte[] getPrimaryRowkey(Long siteId) {
		byte[] sid = getSiteIdentifier(siteId);
		ByteBuffer rowkey = ByteBuffer.allocate(sid.length);
		rowkey.put(sid);
		return rowkey.array();
	}

	/**
	 * Get zone row key for a given site.
	 * 
	 * @param siteId
	 * @return
	 */
	public static byte[] getZoneRowKey(Long siteId) {
		byte[] sid = getSiteIdentifier(siteId);
		ByteBuffer rowkey = ByteBuffer.allocate(sid.length + 1);
		rowkey.put(sid);
		rowkey.put(SiteRecordType.Zone.getType());
		return rowkey.array();
	}

	/**
	 * Get device assignment row key for a given site.
	 * 
	 * @param siteId
	 * @return
	 */
	public static byte[] getAssignmentRowKey(Long siteId) {
		byte[] sid = getSiteIdentifier(siteId);
		ByteBuffer rowkey = ByteBuffer.allocate(sid.length + 1);
		rowkey.put(sid);
		rowkey.put(SiteRecordType.Assignment.getType());
		return rowkey.array();
	}

	/**
	 * Get key that marks finish of assignment records for a site.
	 * 
	 * @param siteId
	 * @return
	 */
	public static byte[] getAfterAssignmentRowKey(Long siteId) {
		byte[] sid = getSiteIdentifier(siteId);
		ByteBuffer rowkey = ByteBuffer.allocate(sid.length + 1);
		rowkey.put(sid);
		rowkey.put(SiteRecordType.End.getType());
		return rowkey.array();
	}
}
