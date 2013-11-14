/*
 * HBaseZone.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.hbase.device;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sitewhere.core.device.SiteWherePersistence;
import com.sitewhere.hbase.SiteWhereHBaseClient;
import com.sitewhere.hbase.ISiteWhereHBase;
import com.sitewhere.hbase.common.MarshalUtils;
import com.sitewhere.hbase.uid.IdManager;
import com.sitewhere.rest.model.device.Zone;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.SiteWhereSystemException;
import com.sitewhere.spi.device.ISite;
import com.sitewhere.spi.device.IZone;
import com.sitewhere.spi.device.request.IZoneCreateRequest;
import com.sitewhere.spi.error.ErrorCode;
import com.sitewhere.spi.error.ErrorLevel;

/**
 * HBase specifics for dealing with SiteWhere zones.
 * 
 * @author Derek
 */
public class HBaseZone {

	/** Length of site identifier (subset of 8 byte long) */
	public static final int ZONE_IDENTIFIER_LENGTH = 4;

	/**
	 * Create a new zone.
	 * 
	 * @param hbase
	 * @param site
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static IZone createZone(SiteWhereHBaseClient hbase, ISite site, IZoneCreateRequest request)
			throws SiteWhereException {
		Long siteId = IdManager.getInstance().getSiteKeys().getValue(site.getToken());
		if (siteId == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidSiteToken, ErrorLevel.ERROR);
		}
		Long zoneId = HBaseSite.allocateNextZoneId(hbase, siteId);
		byte[] rowkey = getPrimaryRowkey(siteId, zoneId);

		// Associate new UUID with zone row key.
		String uuid = IdManager.getInstance().getZoneKeys().createUniqueId(rowkey);

		// Use common processing logic so all backend implementations work the same.
		Zone zone = SiteWherePersistence.zoneCreateLogic(request, site.getToken(), uuid);

		// Serialize as JSON.
		byte[] json = MarshalUtils.marshalJson(zone);

		// Create zone record.
		PutRequest put = new PutRequest(ISiteWhereHBase.SITES_TABLE_NAME, rowkey, ISiteWhereHBase.FAMILY_ID,
				ISiteWhereHBase.JSON_CONTENT, json);
		HBasePersistence.syncPut(hbase, put, "Unable to create site.");

		return zone;
	}

	/**
	 * Update an existing zone.
	 * 
	 * @param hbase
	 * @param token
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static Zone updateZone(SiteWhereHBaseClient hbase, String token, IZoneCreateRequest request)
			throws SiteWhereException {
		Zone updated = getZone(hbase, token);

		// Use common update logic so that backend implemetations act the same way.
		SiteWherePersistence.zoneUpdateLogic(request, updated);

		byte[] zoneId = IdManager.getInstance().getZoneKeys().getValue(token);
		byte[] json = MarshalUtils.marshalJson(updated);
		PutRequest put = new PutRequest(ISiteWhereHBase.SITES_TABLE_NAME, zoneId, ISiteWhereHBase.FAMILY_ID,
				ISiteWhereHBase.JSON_CONTENT, json);
		HBasePersistence.syncPut(hbase, put, "Unable to update zone.");
		return updated;
	}

	/**
	 * Get a zone by unique token.
	 * 
	 * @param hbase
	 * @param token
	 * @return
	 * @throws SiteWhereException
	 */
	public static Zone getZone(SiteWhereHBaseClient hbase, String token) throws SiteWhereException {
		byte[] rowkey = IdManager.getInstance().getZoneKeys().getValue(token);
		if (rowkey == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidZoneToken, ErrorLevel.ERROR);
		}

		GetRequest request = new GetRequest(ISiteWhereHBase.SITES_TABLE_NAME, rowkey).family(
				ISiteWhereHBase.FAMILY_ID).qualifier(ISiteWhereHBase.JSON_CONTENT);
		ArrayList<KeyValue> results = HBasePersistence.syncGet(hbase, request,
				"Unable to load zone by token.");
		if (results.size() != 1) {
			throw new SiteWhereException("Expected one JSON entry for zone and found: " + results.size());
		}
		byte[] json = results.get(0).value();
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(json, Zone.class);
		} catch (Throwable e) {
			throw new SiteWhereException("Unable to parse zone JSON.", e);
		}
	}

	/**
	 * Delete an existing zone.
	 * 
	 * @param hbase
	 * @param token
	 * @param force
	 * @return
	 * @throws SiteWhereException
	 */
	public static Zone deleteZone(SiteWhereHBaseClient hbase, String token, boolean force)
			throws SiteWhereException {
		byte[] zoneId = IdManager.getInstance().getZoneKeys().getValue(token);
		if (zoneId == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidZoneToken, ErrorLevel.ERROR);
		}
		Zone existing = getZone(hbase, token);
		existing.setDeleted(true);
		if (force) {
			IdManager.getInstance().getZoneKeys().delete(token);
			DeleteRequest delete = new DeleteRequest(ISiteWhereHBase.SITES_TABLE_NAME, zoneId);
			try {
				hbase.getClient().delete(delete).joinUninterruptibly();
			} catch (Exception e) {
				throw new SiteWhereException("Unable to delete zone.", e);
			}
		} else {
			byte[] marker = { (byte) 0x01 };
			SiteWherePersistence.setUpdatedEntityMetadata(existing);
			byte[] updated = MarshalUtils.marshalJson(existing);
			byte[][] qualifiers = { ISiteWhereHBase.JSON_CONTENT, ISiteWhereHBase.DELETED };
			byte[][] values = { updated, marker };
			PutRequest put = new PutRequest(ISiteWhereHBase.SITES_TABLE_NAME, zoneId,
					ISiteWhereHBase.FAMILY_ID, qualifiers, values);
			HBasePersistence.syncPut(hbase, put, "Unable to set deleted flag for zone.");
		}
		return existing;
	}

	/**
	 * Get primary row key for a given zone.
	 * 
	 * @param siteId
	 * @return
	 */
	public static byte[] getPrimaryRowkey(Long siteId, Long zoneId) {
		byte[] baserow = HBaseSite.getZoneRowKey(siteId);
		byte[] zoneIdBytes = getZoneIdentifier(zoneId);
		ByteBuffer buffer = ByteBuffer.allocate(baserow.length + zoneIdBytes.length);
		buffer.put(baserow);
		buffer.put(zoneIdBytes);
		return buffer.array();
	}

	/**
	 * Truncate zone id value to expected length. This will be a subset of the full 8-bit
	 * long value.
	 * 
	 * @param value
	 * @return
	 */
	public static byte[] getZoneIdentifier(Long value) {
		byte[] bytes = Bytes.fromLong(value);
		byte[] result = new byte[ZONE_IDENTIFIER_LENGTH];
		System.arraycopy(bytes, bytes.length - ZONE_IDENTIFIER_LENGTH, result, 0, ZONE_IDENTIFIER_LENGTH);
		return result;
	}
}