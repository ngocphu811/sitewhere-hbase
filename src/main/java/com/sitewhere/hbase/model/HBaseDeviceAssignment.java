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
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import com.sitewhere.core.device.SiteWherePersistence;
import com.sitewhere.hbase.HBaseConnectivity;
import com.sitewhere.hbase.SiteWhereHBaseConstants;
import com.sitewhere.hbase.common.MarshalUtils;
import com.sitewhere.hbase.uid.IdManager;
import com.sitewhere.rest.model.common.MetadataEntry;
import com.sitewhere.rest.model.common.MetadataProvider;
import com.sitewhere.rest.model.device.DeviceAssignment;
import com.sitewhere.rest.model.device.DeviceLocation;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.SiteWhereSystemException;
import com.sitewhere.spi.common.IMetadataProvider;
import com.sitewhere.spi.device.DeviceAssignmentStatus;
import com.sitewhere.spi.device.IDeviceAssignment;
import com.sitewhere.spi.device.request.IDeviceAssignmentCreateRequest;
import com.sitewhere.spi.device.request.IDeviceLocationCreateRequest;
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

	/** Qualifier for assignment status */
	public static final byte[] ASSIGNMENT_STATUS = Bytes.UTF8("status");

	/** Qualifier for last location */
	public static final byte[] LAST_LOCATION = Bytes.UTF8("location");

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

		SiteWherePersistence.initializeEntityMetadata(newAssignment);
		MetadataProvider.copy(request, newAssignment);

		byte[] json = MarshalUtils.marshalJson(newAssignment);
		byte[][] qualifiers = { SiteWhereHBaseConstants.JSON_CONTENT, ASSIGNMENT_STATUS };
		byte[][] values = { json, DeviceAssignmentStatus.Active.name().getBytes() };
		PutRequest put = new PutRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, rowkey,
				SiteWhereHBaseConstants.FAMILY_ID, qualifiers, values);
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
	public static DeviceAssignment getDeviceAssignment(HBaseConnectivity hbase, String token)
			throws SiteWhereException {
		byte[] rowkey = IdManager.getInstance().getAssignmentKeys().getValue(token);
		if (rowkey == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidDeviceAssignmentToken, ErrorLevel.ERROR);
		}

		GetRequest request = new GetRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, rowkey).family(
				SiteWhereHBaseConstants.FAMILY_ID).qualifier(SiteWhereHBaseConstants.JSON_CONTENT);
		ArrayList<KeyValue> results = HBasePersistence.syncGet(hbase, request,
				"Unable to load device assignment by token.");
		if (results.size() != 1) {
			throw new SiteWhereException("Expected one JSON entry for device assignment and found: "
					+ results.size());
		}
		byte[] json = results.get(0).value();
		return MarshalUtils.unmarshalJson(json, DeviceAssignment.class);
	}

	/**
	 * Update metadata associated with a device assignment.
	 * 
	 * @param hbase
	 * @param token
	 * @param metadata
	 * @return
	 * @throws SiteWhereException
	 */
	public static DeviceAssignment updateDeviceAssignmentMetadata(HBaseConnectivity hbase, String token,
			IMetadataProvider metadata) throws SiteWhereException {
		DeviceAssignment updated = getDeviceAssignment(hbase, token);
		updated.setMetadata(new ArrayList<MetadataEntry>());
		MetadataProvider.copy(metadata, updated);
		SiteWherePersistence.setUpdatedEntityMetadata(updated);

		byte[] rowkey = IdManager.getInstance().getAssignmentKeys().getValue(token);
		byte[] json = MarshalUtils.marshalJson(updated);
		PutRequest put = new PutRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, rowkey,
				SiteWhereHBaseConstants.FAMILY_ID, SiteWhereHBaseConstants.JSON_CONTENT, json);
		HBasePersistence.syncPut(hbase, put, "Unable to update device assignment metadata.");
		return updated;
	}

	/**
	 * Update location associated with device assignment.
	 * 
	 * @param hbase
	 * @param token
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static DeviceAssignment updateDeviceAssignmentLocation(HBaseConnectivity hbase, String token,
			IDeviceLocationCreateRequest request) throws SiteWhereException {
		DeviceAssignment updated = getDeviceAssignment(hbase, token);
		DeviceLocation location = HBaseDeviceEvent.createDeviceLocationForRequest(updated, request);
		updated.setLastLocation(location);
		SiteWherePersistence.setUpdatedEntityMetadata(updated);

		byte[] rowkey = IdManager.getInstance().getAssignmentKeys().getValue(token);
		byte[] json = MarshalUtils.marshalJson(updated);
		byte[] locJson = MarshalUtils.marshalJson(location);
		byte[][] qualifiers = { SiteWhereHBaseConstants.JSON_CONTENT, LAST_LOCATION };
		byte[][] values = { json, locJson };
		PutRequest put = new PutRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, rowkey,
				SiteWhereHBaseConstants.FAMILY_ID, qualifiers, values);
		HBasePersistence.syncPut(hbase, put, "Unable to update device assignment location.");
		return updated;
	}

	/**
	 * Update status for a given device assignment.
	 * 
	 * @param hbase
	 * @param token
	 * @param status
	 * @return
	 * @throws SiteWhereException
	 */
	public static DeviceAssignment updateDeviceAssignmentStatus(HBaseConnectivity hbase, String token,
			DeviceAssignmentStatus status) throws SiteWhereException {
		DeviceAssignment updated = getDeviceAssignment(hbase, token);
		updated.setStatus(status);
		SiteWherePersistence.setUpdatedEntityMetadata(updated);

		byte[] rowkey = IdManager.getInstance().getAssignmentKeys().getValue(token);
		byte[] json = MarshalUtils.marshalJson(updated);
		byte[][] qualifiers = { SiteWhereHBaseConstants.JSON_CONTENT, ASSIGNMENT_STATUS };
		byte[][] values = { json, status.name().getBytes() };
		PutRequest put = new PutRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, rowkey,
				SiteWhereHBaseConstants.FAMILY_ID, qualifiers, values);
		HBasePersistence.syncPut(hbase, put, "Unable to update device assignment status.");
		return updated;
	}

	/**
	 * End a device assignment.
	 * 
	 * @param hbase
	 * @param token
	 * @return
	 * @throws SiteWhereException
	 */
	public static DeviceAssignment endDeviceAssignment(HBaseConnectivity hbase, String token)
			throws SiteWhereException {
		DeviceAssignment updated = getDeviceAssignment(hbase, token);
		updated.setStatus(DeviceAssignmentStatus.Released);
		updated.setReleasedDate(new Date());
		SiteWherePersistence.setUpdatedEntityMetadata(updated);

		// Remove assignment reference from device.
		HBaseDevice.removeDeviceAssignment(hbase, updated.getDeviceHardwareId());

		// Update json and status qualifier.
		byte[] rowkey = IdManager.getInstance().getAssignmentKeys().getValue(token);
		byte[] json = MarshalUtils.marshalJson(updated);
		byte[][] qualifiers = { SiteWhereHBaseConstants.JSON_CONTENT, ASSIGNMENT_STATUS };
		byte[][] values = { json, DeviceAssignmentStatus.Released.name().getBytes() };
		PutRequest put = new PutRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, rowkey,
				SiteWhereHBaseConstants.FAMILY_ID, qualifiers, values);
		HBasePersistence.syncPut(hbase, put, "Unable to update device assignment status.");
		return updated;
	}

	/**
	 * Delete a device assignmant based on token. Depending on 'force' the record will be
	 * physically deleted or a marker qualifier will be added to mark it as deleted. Note:
	 * Physically deleting an assignment can leave orphaned references and should not be
	 * done in a production system!
	 * 
	 * @param hbase
	 * @param token
	 * @param force
	 * @return
	 * @throws SiteWhereException
	 */
	public static IDeviceAssignment deleteDeviceAssignment(HBaseConnectivity hbase, String token,
			boolean force) throws SiteWhereException {
		byte[] assnId = IdManager.getInstance().getAssignmentKeys().getValue(token);
		if (assnId == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidDeviceAssignmentToken, ErrorLevel.ERROR);
		}
		DeviceAssignment existing = getDeviceAssignment(hbase, token);
		existing.setDeleted(true);
		HBaseDevice.removeDeviceAssignment(hbase, existing.getDeviceHardwareId());
		if (force) {
			IdManager.getInstance().getAssignmentKeys().delete(token);
			DeleteRequest delete = new DeleteRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, assnId);
			try {
				hbase.getClient().delete(delete).joinUninterruptibly();
			} catch (Exception e) {
				throw new SiteWhereException("Unable to delete device.", e);
			}
		} else {
			byte[] marker = { (byte) 0x01 };
			SiteWherePersistence.setUpdatedEntityMetadata(existing);
			byte[] updated = MarshalUtils.marshalJson(existing);
			byte[][] qualifiers = { SiteWhereHBaseConstants.JSON_CONTENT, SiteWhereHBaseConstants.DELETED };
			byte[][] values = { updated, marker };
			PutRequest put = new PutRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, assnId,
					SiteWhereHBaseConstants.FAMILY_ID, qualifiers, values);
			HBasePersistence.syncPut(hbase, put, "Unable to set deleted flag for device assignment.");
		}
		return existing;
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