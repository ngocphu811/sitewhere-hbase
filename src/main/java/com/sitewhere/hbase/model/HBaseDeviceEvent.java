/*
 * HBaseDeviceEvent.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.hbase.model;

import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.log4j.Logger;
import org.hbase.async.Bytes;
import org.hbase.async.PutRequest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sitewhere.core.device.Utils;
import com.sitewhere.hbase.HBaseConnectivity;
import com.sitewhere.hbase.SiteWhereHBaseConstants;
import com.sitewhere.hbase.uid.IdManager;
import com.sitewhere.rest.model.common.MetadataProvider;
import com.sitewhere.rest.model.device.DeviceMeasurements;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.SiteWhereSystemException;
import com.sitewhere.spi.common.IMeasurementEntry;
import com.sitewhere.spi.device.IDeviceAssignment;
import com.sitewhere.spi.device.IDeviceMeasurements;
import com.sitewhere.spi.device.request.IDeviceEventCreateRequest;
import com.sitewhere.spi.device.request.IDeviceMeasurementsCreateRequest;
import com.sitewhere.spi.error.ErrorCode;
import com.sitewhere.spi.error.ErrorLevel;
import com.stumbleupon.async.Callback;

/**
 * HBase specifics for dealing with SiteWhere device events.
 * 
 * @author Derek
 */
public class HBaseDeviceEvent {

	/** Static logger instance */
	private static Logger LOGGER = Logger.getLogger(HBaseDeviceEvent.class);

	/** Time interval in milliseconds to use for buckets */
	private static final int BUCKET_INTERVAL = 60 * 60 * 1000;

	/**
	 * Create a new device measurements entry for an assignment.
	 * 
	 * @param hbase
	 * @param assignment
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static IDeviceMeasurements createDeviceMeasurements(HBaseConnectivity hbase,
			IDeviceAssignment assignment, IDeviceMeasurementsCreateRequest request) throws SiteWhereException {
		long time = getEventTime(request);
		byte[] rowkey = getRowKey(assignment.getToken(), time);
		byte[] qualifier = getQualifier(DeviceAssignmentRecordType.Measurement, time);

		DeviceMeasurements measurements = new DeviceMeasurements();
		measurements.setSiteToken(assignment.getSiteToken());
		measurements.setDeviceAssignmentToken(assignment.getToken());
		measurements.setAssetName(Utils.getAssetNameForAssignment(assignment));
		measurements.setEventDate(request.getEventDate());
		measurements.setReceivedDate(new Date());
		for (IMeasurementEntry entry : request.getMeasurements()) {
			measurements.addOrReplaceMeasurement(entry.getName(), entry.getValue());
		}
		MetadataProvider.copy(request, measurements);

		// Serialize as JSON.
		ObjectMapper mapper = new ObjectMapper();
		String json = null;
		try {
			json = mapper.writeValueAsString(measurements);
		} catch (JsonProcessingException e) {
			throw new SiteWhereException("Could not marshal device measurements as JSON.", e);
		}

		// Create device measurements record. Fire and forget so errors only hit callback.
		PutRequest put = new PutRequest(SiteWhereHBaseConstants.SITES_TABLE_NAME, rowkey,
				SiteWhereHBaseConstants.FAMILY_ID, qualifier, json.getBytes());
		hbase.getClient().put(put).addErrback(new PutFailedCallback());

		return measurements;
	}

	/**
	 * Get the event time used to calculate row key and qualifier.
	 * 
	 * @param event
	 * @return
	 */
	protected static long getEventTime(IDeviceEventCreateRequest event) {
		return (event.getEventDate() != null) ? event.getEventDate().getTime() : System.currentTimeMillis();
	}

	/**
	 * Get row key for a given event type and time.
	 * 
	 * @param assnToken
	 * @param eventType
	 * @param time
	 * @return
	 * @throws SiteWhereException
	 */
	public static byte[] getRowKey(String assnToken, long time) throws SiteWhereException {
		byte[] assnKey = IdManager.getInstance().getAssignmentKeys().getValue(assnToken);
		if (assnKey == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidDeviceAssignmentToken, ErrorLevel.ERROR);
		}
		long bucket = time - (time % BUCKET_INTERVAL);
		byte[] bucketBytes = Bytes.fromLong(bucket);
		ByteBuffer buffer = ByteBuffer.allocate(assnKey.length + 4);
		buffer.put(assnKey);
		buffer.put(bucketBytes[4]);
		buffer.put(bucketBytes[5]);
		buffer.put(bucketBytes[6]);
		buffer.put(bucketBytes[7]);
		return buffer.array();
	}

	/**
	 * Get column qualifier for storing the event.
	 * 
	 * @param type
	 * @param time
	 * @return
	 */
	public static byte[] getQualifier(DeviceAssignmentRecordType eventType, long time) {
		long offset = time % BUCKET_INTERVAL;
		byte[] offsetBytes = Bytes.fromLong(offset);
		ByteBuffer buffer = ByteBuffer.allocate(4);
		buffer.put(offsetBytes[5]);
		buffer.put(offsetBytes[6]);
		buffer.put(offsetBytes[7]);
		buffer.put(eventType.getType());
		return buffer.array();
	}

	/**
	 * Since events are sent to the backend in a fire-and-forget manner, this class is
	 * used to handle "out of band" errors in a consistent way.
	 */
	static class PutFailedCallback implements Callback<Object, Exception> {
		public Object call(final Exception e) {
			LOGGER.error("Unable to write event to HBase.", e);
			return e;
		}
	}
}