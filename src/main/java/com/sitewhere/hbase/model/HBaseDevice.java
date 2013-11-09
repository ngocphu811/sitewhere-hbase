/*
 * HBaseDevice.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.hbase.model;

import java.util.ArrayList;

import javax.servlet.http.HttpServletResponse;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sitewhere.dao.mongodb.MongoPersistence;
import com.sitewhere.hbase.HBaseConnectivity;
import com.sitewhere.hbase.SiteWhereHBaseConstants;
import com.sitewhere.hbase.uid.IdManager;
import com.sitewhere.rest.model.common.MetadataProvider;
import com.sitewhere.rest.model.device.Device;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.SiteWhereSystemException;
import com.sitewhere.spi.device.IDevice;
import com.sitewhere.spi.device.request.IDeviceCreateRequest;
import com.sitewhere.spi.error.ErrorCode;
import com.sitewhere.spi.error.ErrorLevel;

/**
 * HBase specifics for dealing with SiteWhere devices.
 * 
 * @author Derek
 */
public class HBaseDevice {

	/** Length of device identifier (subset of 8 byte long) */
	public static final int DEVICE_IDENTIFIER_LENGTH = 4;

	/** Column qualifier for site JSON content */
	public static final byte[] JSON_CONTENT = Bytes.UTF8("json");

	/**
	 * Create a new device.
	 * 
	 * @param hbase
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static IDevice createDevice(HBaseConnectivity hbase, IDeviceCreateRequest request)
			throws SiteWhereException {
		Long existing = IdManager.getInstance().getDeviceKeys().getValue(request.getHardwareId());
		if (existing != null) {
			throw new SiteWhereSystemException(ErrorCode.DuplicateHardwareId, ErrorLevel.ERROR,
					HttpServletResponse.SC_CONFLICT);
		}
		Long value = IdManager.getInstance().getDeviceKeys().getNextCounterValue();
		IdManager.getInstance().getDeviceKeys().create(request.getHardwareId(), value);
		byte[] primary = getPrimaryRowkey(value);

		Device newDevice = new Device();
		newDevice.setAssetId(request.getAssetId());
		newDevice.setHardwareId(request.getHardwareId());
		newDevice.setComments(request.getComments());

		MetadataProvider.copy(request, newDevice);
		MongoPersistence.initializeEntityMetadata(newDevice);

		// Serialize as JSON.
		ObjectMapper mapper = new ObjectMapper();
		String json = null;
		try {
			json = mapper.writeValueAsString(newDevice);
		} catch (JsonProcessingException e) {
			throw new SiteWhereException("Could not marshal device as JSON.", e);
		}

		// Create primary device record.
		PutRequest put = new PutRequest(SiteWhereHBaseConstants.DEVICES_TABLE_NAME, primary,
				SiteWhereHBaseConstants.FAMILY_ID, JSON_CONTENT, json.getBytes());
		HBasePersistence.syncPut(hbase, put, "Unable to create device.");

		return newDevice;
	}

	/**
	 * Get a device by unique hardware id.
	 * 
	 * @param hbase
	 * @param hardwareId
	 * @return
	 * @throws SiteWhereException
	 */
	public static IDevice getDeviceByHardwareId(HBaseConnectivity hbase, String hardwareId)
			throws SiteWhereException {
		Long deviceId = IdManager.getInstance().getDeviceKeys().getValue(hardwareId);
		if (deviceId == null) {
			return null;
		}

		// Find row key based on value associated with hardware id.
		byte[] primary = getPrimaryRowkey(deviceId);
		GetRequest request = new GetRequest(SiteWhereHBaseConstants.DEVICES_TABLE_NAME, primary).family(
				SiteWhereHBaseConstants.FAMILY_ID).qualifier(JSON_CONTENT);
		ArrayList<KeyValue> results = HBasePersistence.syncGet(hbase, request,
				"Unable to load device by hardware id.");
		if (results.size() != 1) {
			throw new SiteWhereException("Expected one JSON entry for device and found: " + results.size());
		}
		byte[] json = results.get(0).value();
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(json, Device.class);
		} catch (Throwable e) {
			throw new SiteWhereException("Unable to parse device JSON.", e);
		}
	}

	/**
	 * Get the unique device identifier based on the long value associated with the device
	 * UUID. This will be a subset of the full 8-bit long value.
	 * 
	 * @param value
	 * @return
	 */
	public static byte[] getDeviceIdentifier(Long value) {
		byte[] bytes = Bytes.fromLong(value);
		byte[] result = new byte[DEVICE_IDENTIFIER_LENGTH];
		System.arraycopy(bytes, bytes.length - DEVICE_IDENTIFIER_LENGTH, result, 0, DEVICE_IDENTIFIER_LENGTH);
		return result;
	}

	/**
	 * Get primary row key for a given site.
	 * 
	 * @param siteId
	 * @return
	 */
	public static byte[] getPrimaryRowkey(Long deviceId) {
		byte[] did = getDeviceIdentifier(deviceId);
		return did;
	}
}