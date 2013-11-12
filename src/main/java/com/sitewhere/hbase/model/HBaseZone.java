/*
 * HBaseZone.java 
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

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sitewhere.hbase.HBaseConnectivity;
import com.sitewhere.hbase.SiteWhereHBaseConstants;
import com.sitewhere.hbase.uid.IdManager;
import com.sitewhere.rest.model.common.MetadataProvider;
import com.sitewhere.rest.model.device.Zone;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.SiteWhereSystemException;
import com.sitewhere.spi.common.ILocation;
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
	public static IZone createZone(HBaseConnectivity hbase, ISite site, IZoneCreateRequest request)
			throws SiteWhereException {
		Long siteId = IdManager.getInstance().getSiteKeys().getValue(site.getToken());
		if (siteId == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidSiteToken, ErrorLevel.ERROR);
		}
		Long zoneId = HBaseSite.allocateNextZoneId(hbase, siteId);
		byte[] rowkey = getPrimaryRowkey(siteId, zoneId);

		// Associate new UUID with zone row key.
		String uuid = IdManager.getInstance().getZoneKeys().createUniqueId(rowkey);

		// Create zone to marshal as JSON.
		Zone zone = new Zone();
		zone.setToken(uuid);
		zone.setSiteToken(site.getToken());
		zone.setName(request.getName());
		zone.setBorderColor(request.getBorderColor());
		zone.setFillColor(request.getFillColor());
		zone.setOpacity(request.getOpacity());

		HBasePersistence.initializeEntityMetadata(zone);
		MetadataProvider.copy(request, zone);

		for (ILocation coordinate : request.getCoordinates()) {
			zone.getCoordinates().add(coordinate);
		}

		// Serialize as JSON.
		ObjectMapper mapper = new ObjectMapper();
		String json = null;
		try {
			json = mapper.writeValueAsString(zone);
		} catch (JsonProcessingException e) {
			throw new SiteWhereException("Could not marshal zone as JSON.", e);
		}

		// Create zone record.
		PutRequest put = new PutRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, rowkey,
				SiteWhereHBaseConstants.FAMILY_ID, SiteWhereHBaseConstants.JSON_CONTENT, json.getBytes());
		HBasePersistence.syncPut(hbase, put, "Unable to create site.");

		return zone;
	}

	/**
	 * Get a zone by unique token.
	 * 
	 * @param hbase
	 * @param token
	 * @return
	 * @throws SiteWhereException
	 */
	public static IZone getZone(HBaseConnectivity hbase, String token) throws SiteWhereException {
		byte[] rowkey = IdManager.getInstance().getZoneKeys().getValue(token);
		if (rowkey == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidZoneToken, ErrorLevel.ERROR);
		}

		GetRequest request = new GetRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, rowkey).family(
				SiteWhereHBaseConstants.FAMILY_ID).qualifier(SiteWhereHBaseConstants.JSON_CONTENT);
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