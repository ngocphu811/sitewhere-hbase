/*
 * HBaseDevice.java 
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
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

import com.sitewhere.core.SiteWherePersistence;
import com.sitewhere.hbase.ISiteWhereHBase;
import com.sitewhere.hbase.SiteWhereHBaseClient;
import com.sitewhere.hbase.common.MarshalUtils;
import com.sitewhere.hbase.common.Pager;
import com.sitewhere.hbase.uid.IdManager;
import com.sitewhere.rest.model.common.MetadataProvider;
import com.sitewhere.rest.model.device.Device;
import com.sitewhere.rest.model.device.DeviceAssignment;
import com.sitewhere.rest.service.search.SearchResults;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.SiteWhereSystemException;
import com.sitewhere.spi.common.ISearchCriteria;
import com.sitewhere.spi.device.IDevice;
import com.sitewhere.spi.device.IDeviceAssignment;
import com.sitewhere.spi.device.request.IDeviceCreateRequest;
import com.sitewhere.spi.error.ErrorCode;
import com.sitewhere.spi.error.ErrorLevel;

/**
 * HBase specifics for dealing with SiteWhere devices.
 * 
 * @author Derek
 */
public class HBaseDevice {

	/** Static logger instance */
	private static Logger LOGGER = Logger.getLogger(HBaseDevice.class);

	/** Length of device identifier (subset of 8 byte long) */
	public static final int DEVICE_IDENTIFIER_LENGTH = 4;

	/** Byte that indicates an assignment history entry qualifier */
	public static final byte ASSIGNMENT_HISTORY_INDICATOR = (byte) 0x01;

	/** Column qualifier for current device assignment */
	public static final byte[] CURRENT_ASSIGNMENT = Bytes.UTF8("assignment");

	/**
	 * Create a new device.
	 * 
	 * @param hbase
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static IDevice createDevice(SiteWhereHBaseClient hbase, IDeviceCreateRequest request)
			throws SiteWhereException {
		Long existing = IdManager.getInstance().getDeviceKeys().getValue(request.getHardwareId());
		if (existing != null) {
			throw new SiteWhereSystemException(ErrorCode.DuplicateHardwareId, ErrorLevel.ERROR,
					HttpServletResponse.SC_CONFLICT);
		}
		Long value = IdManager.getInstance().getDeviceKeys().getNextCounterValue();
		IdManager.getInstance().getDeviceKeys().create(request.getHardwareId(), value);

		Device newDevice = new Device();
		newDevice.setAssetId(request.getAssetId());
		newDevice.setHardwareId(request.getHardwareId());
		newDevice.setComments(request.getComments());

		MetadataProvider.copy(request, newDevice);
		SiteWherePersistence.initializeEntityMetadata(newDevice);

		return putDeviceJson(hbase, newDevice);
	}

	/**
	 * Update an existing device.
	 * 
	 * @param hbase
	 * @param hardwareId
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static IDevice updateDevice(SiteWhereHBaseClient hbase, String hardwareId,
			IDeviceCreateRequest request) throws SiteWhereException {

		// Can not update the hardware id on a device.
		if ((request.getHardwareId() != null) && (!request.getHardwareId().equals(hardwareId))) {
			throw new SiteWhereSystemException(ErrorCode.DeviceHardwareIdCanNotBeChanged, ErrorLevel.ERROR,
					HttpServletResponse.SC_BAD_REQUEST);
		}

		// Copy any non-null fields.
		Device updatedDevice = getDeviceByHardwareId(hbase, hardwareId);
		if (request.getAssetId() != null) {
			updatedDevice.setAssetId(request.getAssetId());
		}
		if (request.getComments() != null) {
			updatedDevice.setComments(request.getComments());
		}
		if ((request.getMetadata() != null) && (request.getMetadata().size() > 0)) {
			updatedDevice.getMetadata().clear();
			MetadataProvider.copy(request, updatedDevice);
		}
		SiteWherePersistence.setUpdatedEntityMetadata(updatedDevice);

		return putDeviceJson(hbase, updatedDevice);
	}

	/**
	 * List devices that meet the given criteria.
	 * 
	 * @param hbase
	 * @param includeDeleted
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	public static SearchResults<IDevice> listDevices(SiteWhereHBaseClient hbase, boolean includeDeleted,
			ISearchCriteria criteria) throws SiteWhereException {
		Pager<byte[]> matches = getFilteredDevices(hbase, includeDeleted, false, criteria);
		List<IDevice> response = new ArrayList<IDevice>();
		for (byte[] json : matches.getResults()) {
			response.add(MarshalUtils.unmarshalJson(json, Device.class));
		}
		return new SearchResults<IDevice>(response, matches.getTotal());
	}

	/**
	 * List devices that do not have a current assignment.
	 * 
	 * @param hbase
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	public static SearchResults<IDevice> listUnassignedDevices(SiteWhereHBaseClient hbase,
			ISearchCriteria criteria) throws SiteWhereException {
		Pager<byte[]> matches = getFilteredDevices(hbase, false, true, criteria);
		List<IDevice> response = new ArrayList<IDevice>();
		for (byte[] json : matches.getResults()) {
			response.add(MarshalUtils.unmarshalJson(json, Device.class));
		}
		return new SearchResults<IDevice>(response, matches.getTotal());
	}

	/**
	 * Get a list of devices filtered with certain criteria.
	 * 
	 * @param hbase
	 * @param includeDeleted
	 * @param excludeAssigned
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	protected static Pager<byte[]> getFilteredDevices(SiteWhereHBaseClient hbase, boolean includeDeleted,
			boolean excludeAssigned, ISearchCriteria criteria) throws SiteWhereException {
		Scanner scanner = hbase.getClient().newScanner(ISiteWhereHBase.DEVICES_TABLE_NAME);
		try {
			Pager<byte[]> pager = new Pager<byte[]>(criteria);
			ArrayList<ArrayList<KeyValue>> results;
			while ((results = scanner.nextRows().joinUninterruptibly()) != null) {
				for (ArrayList<KeyValue> row : results) {
					boolean shouldAdd = true;
					KeyValue jsonColumn = null;
					for (KeyValue column : row) {
						byte[] qualifier = column.qualifier();
						if ((Bytes.equals(CURRENT_ASSIGNMENT, qualifier)) && (excludeAssigned)) {
							shouldAdd = false;
						}
						if ((Bytes.equals(ISiteWhereHBase.DELETED, qualifier)) && (!includeDeleted)) {
							shouldAdd = false;
						}
						if (Bytes.equals(ISiteWhereHBase.JSON_CONTENT, qualifier)) {
							jsonColumn = column;
						}
					}
					if ((shouldAdd) && (jsonColumn != null)) {
						pager.process(jsonColumn.value());
					}
				}
			}
			return pager;
		} catch (Exception e) {
			throw new SiteWhereException("Error scanning results for listing devices.", e);
		} finally {
			try {
				scanner.close().joinUninterruptibly();
			} catch (Exception e) {
				LOGGER.error("Error shutting down scanner for listing devices.", e);
			}
		}
	}

	/**
	 * Save the JSON representation of a device.
	 * 
	 * @param hbase
	 * @param device
	 * @return
	 * @throws SiteWhereException
	 */
	public static Device putDeviceJson(SiteWhereHBaseClient hbase, Device device) throws SiteWhereException {
		Long value = IdManager.getInstance().getDeviceKeys().getValue(device.getHardwareId());
		if (value == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidHardwareId, ErrorLevel.ERROR);
		}
		byte[] primary = getPrimaryRowkey(value);
		byte[] json = MarshalUtils.marshalJson(device);

		// Create primary device record.
		PutRequest put = new PutRequest(ISiteWhereHBase.DEVICES_TABLE_NAME, primary,
				ISiteWhereHBase.FAMILY_ID, ISiteWhereHBase.JSON_CONTENT, json);
		HBasePersistence.syncPut(hbase, put, "Unable to set JSON for device.");

		return device;
	}

	/**
	 * Get a device by unique hardware id.
	 * 
	 * @param hbase
	 * @param hardwareId
	 * @return
	 * @throws SiteWhereException
	 */
	public static Device getDeviceByHardwareId(SiteWhereHBaseClient hbase, String hardwareId)
			throws SiteWhereException {
		Long deviceId = IdManager.getInstance().getDeviceKeys().getValue(hardwareId);
		if (deviceId == null) {
			return null;
		}

		// Find row key based on value associated with hardware id.
		byte[] primary = getPrimaryRowkey(deviceId);
		GetRequest request = new GetRequest(ISiteWhereHBase.DEVICES_TABLE_NAME, primary).family(
				ISiteWhereHBase.FAMILY_ID).qualifier(ISiteWhereHBase.JSON_CONTENT);
		ArrayList<KeyValue> results = HBasePersistence.syncGet(hbase, request,
				"Unable to load device by hardware id.");
		if (results.size() != 1) {
			throw new SiteWhereException("Expected one JSON entry for device and found: " + results.size());
		}
		byte[] json = results.get(0).value();
		return MarshalUtils.unmarshalJson(json, Device.class);
	}

	/**
	 * Delete a device based on hardware id. Depending on 'force' the record will be
	 * physically deleted or a marker qualifier will be added to mark it as deleted. Note:
	 * Physically deleting a device can leave orphaned references and should not be done
	 * in a production system!
	 * 
	 * @param hbase
	 * @param hardwareId
	 * @param force
	 * @return
	 * @throws SiteWhereException
	 */
	public static IDevice deleteDevice(SiteWhereHBaseClient hbase, String hardwareId, boolean force)
			throws SiteWhereException {
		Long deviceId = IdManager.getInstance().getDeviceKeys().getValue(hardwareId);
		if (deviceId == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidHardwareId, ErrorLevel.ERROR);
		}
		Device existing = getDeviceByHardwareId(hbase, hardwareId);
		existing.setDeleted(true);
		byte[] primary = getPrimaryRowkey(deviceId);
		if (force) {
			IdManager.getInstance().getDeviceKeys().delete(hardwareId);
			DeleteRequest delete = new DeleteRequest(ISiteWhereHBase.DEVICES_TABLE_NAME, primary);
			try {
				hbase.getClient().delete(delete).joinUninterruptibly();
			} catch (Exception e) {
				throw new SiteWhereException("Unable to delete device.", e);
			}
		} else {
			byte[] marker = { (byte) 0x01 };
			SiteWherePersistence.setUpdatedEntityMetadata(existing);
			byte[] updated = MarshalUtils.marshalJson(existing);
			byte[][] qualifiers = { ISiteWhereHBase.JSON_CONTENT, ISiteWhereHBase.DELETED };
			byte[][] values = { updated, marker };
			PutRequest put = new PutRequest(ISiteWhereHBase.DEVICES_TABLE_NAME, primary,
					ISiteWhereHBase.FAMILY_ID, qualifiers, values);
			HBasePersistence.syncPut(hbase, put, "Unable to set deleted flag for device.");
		}
		return existing;
	}

	/**
	 * Get the current device assignment id if assigned or null if not assigned.
	 * 
	 * @param hbase
	 * @param hardwareId
	 * @return
	 * @throws SiteWhereException
	 */
	public static String getCurrentAssignmentId(SiteWhereHBaseClient hbase, String hardwareId)
			throws SiteWhereException {
		Long deviceId = IdManager.getInstance().getDeviceKeys().getValue(hardwareId);
		if (deviceId == null) {
			return null;
		}
		byte[] primary = getPrimaryRowkey(deviceId);
		GetRequest request = new GetRequest(ISiteWhereHBase.DEVICES_TABLE_NAME, primary).family(
				ISiteWhereHBase.FAMILY_ID).qualifier(CURRENT_ASSIGNMENT);
		ArrayList<KeyValue> results = HBasePersistence.syncGet(hbase, request,
				"Unable to load current device assignment value.");
		if (results.isEmpty()) {
			return null;
		} else if (results.size() == 1) {
			return new String(results.get(0).value());
		} else {
			throw new SiteWhereException("Expected one current assignment entry for device and found: "
					+ results.size());
		}
	}

	/**
	 * Set the current device assignment for a device.
	 * 
	 * @param hbase
	 * @param hardwareId
	 * @param assignmentToken
	 * @throws SiteWhereException
	 */
	public static void setDeviceAssignment(SiteWhereHBaseClient hbase, String hardwareId,
			String assignmentToken) throws SiteWhereException {
		String existing = getCurrentAssignmentId(hbase, hardwareId);
		if (existing != null) {
			throw new SiteWhereSystemException(ErrorCode.DeviceAlreadyAssigned, ErrorLevel.ERROR);
		}

		// Load object to update assignment token.
		Device updated = getDeviceByHardwareId(hbase, hardwareId);
		updated.setAssignmentToken(assignmentToken);
		byte[] json = MarshalUtils.marshalJson(updated);

		Long deviceId = IdManager.getInstance().getDeviceKeys().getValue(hardwareId);
		if (deviceId == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidHardwareId, ErrorLevel.ERROR);
		}
		byte[] primary = getPrimaryRowkey(deviceId);
		byte[] assnHistory = getNextDeviceAssignmentHistoryKey();
		byte[][] qualifiers = { ISiteWhereHBase.JSON_CONTENT, CURRENT_ASSIGNMENT, assnHistory };
		byte[][] values = { json, assignmentToken.getBytes(), assignmentToken.getBytes() };
		PutRequest put = new PutRequest(ISiteWhereHBase.DEVICES_TABLE_NAME, primary,
				ISiteWhereHBase.FAMILY_ID, qualifiers, values);
		HBasePersistence.syncPut(hbase, put, "Unable to update device assignment.");
	}

	/**
	 * Removes the device assignment row if present.
	 * 
	 * @param hbase
	 * @param hardwareId
	 * @throws SiteWhereException
	 */
	public static void removeDeviceAssignment(SiteWhereHBaseClient hbase, String hardwareId)
			throws SiteWhereException {
		Long deviceId = IdManager.getInstance().getDeviceKeys().getValue(hardwareId);
		if (deviceId == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidHardwareId, ErrorLevel.ERROR);
		}
		byte[] primary = getPrimaryRowkey(deviceId);

		Device updated = getDeviceByHardwareId(hbase, hardwareId);
		updated.setAssignmentToken(null);
		byte[] json = MarshalUtils.marshalJson(updated);
		PutRequest put = new PutRequest(ISiteWhereHBase.DEVICES_TABLE_NAME, primary,
				ISiteWhereHBase.FAMILY_ID, ISiteWhereHBase.JSON_CONTENT, json);
		try {
			hbase.getClient().put(put).joinUninterruptibly();
		} catch (Exception e) {
			throw new SiteWhereException("Unable to delete assignment token for device.", e);
		}

		DeleteRequest delete = new DeleteRequest(ISiteWhereHBase.DEVICES_TABLE_NAME, primary,
				ISiteWhereHBase.FAMILY_ID, CURRENT_ASSIGNMENT);
		try {
			hbase.getClient().delete(delete).joinUninterruptibly();
		} catch (Exception e) {
			throw new SiteWhereException("Unable to delete device assignment indicator for device.", e);
		}
	}

	/**
	 * Get the assignment history for a device.
	 * 
	 * @param hbase
	 * @param hardwareId
	 * @return
	 * @throws SiteWhereException
	 */
	public static SearchResults<IDeviceAssignment> getDeviceAssignmentHistory(SiteWhereHBaseClient hbase,
			String hardwareId) throws SiteWhereException {
		Long deviceId = IdManager.getInstance().getDeviceKeys().getValue(hardwareId);
		if (deviceId == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidHardwareId, ErrorLevel.ERROR);
		}
		byte[] primary = getPrimaryRowkey(deviceId);
		GetRequest get = new GetRequest(ISiteWhereHBase.DEVICES_TABLE_NAME, primary);
		try {
			List<IDeviceAssignment> results = new ArrayList<IDeviceAssignment>();
			ArrayList<KeyValue> columns = hbase.getClient().get(get).joinUninterruptibly();
			for (KeyValue column : columns) {
				byte[] qualifier = column.qualifier();
				if (qualifier[0] == ASSIGNMENT_HISTORY_INDICATOR) {
					String token = new String(column.value());
					DeviceAssignment assn = HBaseDeviceAssignment.getDeviceAssignment(hbase, token);
					results.add(assn);
				}
			}
			return new SearchResults<IDeviceAssignment>(results);
		} catch (Exception e) {
			throw new SiteWhereException("Unable to load device assignment history.", e);
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

	/**
	 * Creates key with an indicator byte followed by the inverted timestamp to order
	 * assignments in most recent to least recent order.
	 * 
	 * @return
	 */
	public static byte[] getNextDeviceAssignmentHistoryKey() {
		long time = System.currentTimeMillis() / 1000;
		byte[] timeBytes = Bytes.fromLong(time);
		ByteBuffer buffer = ByteBuffer.allocate(5);
		buffer.put(ASSIGNMENT_HISTORY_INDICATOR);
		buffer.put((byte) ~timeBytes[4]);
		buffer.put((byte) ~timeBytes[5]);
		buffer.put((byte) ~timeBytes[6]);
		buffer.put((byte) ~timeBytes[7]);
		return buffer.array();
	}
}