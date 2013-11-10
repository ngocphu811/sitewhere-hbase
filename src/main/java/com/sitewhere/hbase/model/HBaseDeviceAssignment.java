/*
 * HBaseDeviceAssignment.java 
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
import java.util.Date;

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
import com.sitewhere.rest.model.device.DeviceAssignment;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.SiteWhereSystemException;
import com.sitewhere.spi.device.DeviceAssignmentStatus;
import com.sitewhere.spi.device.IDeviceAssignment;
import com.sitewhere.spi.device.request.IDeviceAssignmentCreateRequest;
import com.sitewhere.spi.error.ErrorCode;
import com.sitewhere.spi.error.ErrorLevel;

/**
 * HBase specifics for dealing with SiteWhere device assignments.
 * 
 * @author Derek
 */
public class HBaseDeviceAssignment {

	/** Length of device identifier (subset of 8 byte long) */
	public static final int ASSIGNMENT_IDENTIFIER_LENGTH = 4;

	/** Column qualifier for site JSON content */
	public static final byte[] JSON_CONTENT = Bytes.UTF8("json");

	/**
	 * Create a new device assignment.
	 * 
	 * @param hbase
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static IDeviceAssignment createDeviceAssignment(HBaseConnectivity hbase,
			IDeviceAssignmentCreateRequest request) throws SiteWhereException {
		Long siteId = IdManager.getInstance().getSiteKeys().getValue(request.getSiteToken());
		if (siteId == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidSiteToken, ErrorLevel.ERROR);
		}
		String existing = HBaseDevice.getCurrentAssignmentId(hbase, request.getDeviceHardwareId());
		if (existing != null) {
			throw new SiteWhereSystemException(ErrorCode.DeviceAlreadyAssigned, ErrorLevel.ERROR);
		}
		byte[] baserow = HBaseSite.getAssignmentRowKey(siteId);
		Long assnId = HBaseSite.allocateNextAssignmentId(hbase, siteId);
		byte[] assnIdBytes = getAssignmentIdentifier(assnId);
		ByteBuffer buffer = ByteBuffer.allocate(baserow.length + assnIdBytes.length);
		buffer.put(baserow);
		buffer.put(assnIdBytes);
		byte[] rowkey = buffer.array();

		// Associate new UUID with assignment row key.
		String uuid = IdManager.getInstance().getAssignmentKeys().createUniqueId(rowkey);

		// Create device assignment for JSON.
		DeviceAssignment newAssignment = new DeviceAssignment();
		newAssignment.setToken(uuid);
		newAssignment.setSiteToken(request.getSiteToken());
		newAssignment.setDeviceHardwareId(request.getDeviceHardwareId());
		newAssignment.setAssignmentType(request.getAssignmentType());
		newAssignment.setAssetId(request.getAssetId());
		newAssignment.setActiveDate(new Date());
		newAssignment.setStatus(DeviceAssignmentStatus.Active);

		HBasePersistence.initializeEntityMetadata(newAssignment);
		MetadataProvider.copy(request, newAssignment);

		// Serialize as JSON.
		ObjectMapper mapper = new ObjectMapper();
		String json = null;
		try {
			json = mapper.writeValueAsString(newAssignment);
		} catch (JsonProcessingException e) {
			throw new SiteWhereException("Could not marshal device assignment as JSON.", e);
		}

		// Create zone record.
		PutRequest put = new PutRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, rowkey,
				SiteWhereHBaseConstants.FAMILY_ID, JSON_CONTENT, json.getBytes());
		HBasePersistence.syncPut(hbase, put, "Unable to create device assignment.");

		// Set the back reference from the device that indicates it is currently assigned.
		HBaseDevice.setDeviceAssignment(hbase, request.getDeviceHardwareId(), uuid);

		return newAssignment;
	}

	/**
	 * Get a device assignment based on its unique token.
	 * 
	 * @param hbase
	 * @param token
	 * @return
	 * @throws SiteWhereException
	 */
	public static IDeviceAssignment getDeviceAssignment(HBaseConnectivity hbase, String token)
			throws SiteWhereException {
		byte[] rowkey = IdManager.getInstance().getAssignmentKeys().getValue(token);
		if (rowkey == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidDeviceAssignmentToken, ErrorLevel.ERROR);
		}

		GetRequest request = new GetRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, rowkey).family(
				SiteWhereHBaseConstants.FAMILY_ID).qualifier(JSON_CONTENT);
		ArrayList<KeyValue> results = HBasePersistence.syncGet(hbase, request,
				"Unable to load device assignment by token.");
		if (results.size() != 1) {
			throw new SiteWhereException("Expected one JSON entry for device assignment and found: "
					+ results.size());
		}
		byte[] json = results.get(0).value();
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(json, DeviceAssignment.class);
		} catch (Throwable e) {
			throw new SiteWhereException("Unable to parse device assignment JSON.", e);
		}
	}

	/**
	 * Truncate assignment id value to expected length. This will be a subset of the full
	 * 8-bit long value.
	 * 
	 * @param value
	 * @return
	 */
	public static byte[] getAssignmentIdentifier(Long value) {
		byte[] bytes = Bytes.fromLong(value);
		byte[] result = new byte[ASSIGNMENT_IDENTIFIER_LENGTH];
		System.arraycopy(bytes, bytes.length - ASSIGNMENT_IDENTIFIER_LENGTH, result, 0,
				ASSIGNMENT_IDENTIFIER_LENGTH);
		return result;
	}
}