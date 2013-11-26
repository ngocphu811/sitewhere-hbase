/*
 * HBaseDeviceEvent.java 
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
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sitewhere.core.SiteWherePersistence;
import com.sitewhere.hbase.ISiteWhereHBase;
import com.sitewhere.hbase.SiteWhereHBaseClient;
import com.sitewhere.hbase.common.MarshalUtils;
import com.sitewhere.hbase.common.Pager;
import com.sitewhere.hbase.uid.IdManager;
import com.sitewhere.rest.model.device.DeviceAlert;
import com.sitewhere.rest.model.device.DeviceEvent;
import com.sitewhere.rest.model.device.DeviceLocation;
import com.sitewhere.rest.model.device.DeviceMeasurements;
import com.sitewhere.rest.service.search.SearchResults;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.SiteWhereSystemException;
import com.sitewhere.spi.common.IDateRangeSearchCriteria;
import com.sitewhere.spi.device.IDeviceAlert;
import com.sitewhere.spi.device.IDeviceAssignment;
import com.sitewhere.spi.device.IDeviceEvent;
import com.sitewhere.spi.device.IDeviceLocation;
import com.sitewhere.spi.device.IDeviceMeasurements;
import com.sitewhere.spi.device.request.IDeviceAlertCreateRequest;
import com.sitewhere.spi.device.request.IDeviceEventCreateRequest;
import com.sitewhere.spi.device.request.IDeviceLocationCreateRequest;
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

	/** Time interval in seconds to use for buckets */
	private static final int BUCKET_INTERVAL = 60 * 60;

	/**
	 * Create a new device measurements entry for an assignment.
	 * 
	 * @param hbase
	 * @param assignment
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static IDeviceMeasurements createDeviceMeasurements(SiteWhereHBaseClient hbase,
			IDeviceAssignment assignment, IDeviceMeasurementsCreateRequest request) throws SiteWhereException {
		long time = getEventTime(request);
		byte[] assnKey = IdManager.getInstance().getAssignmentKeys().getValue(assignment.getToken());
		if (assnKey == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidDeviceAssignmentToken, ErrorLevel.ERROR);
		}
		byte[] rowkey = getRowKey(assnKey, time);
		byte[] qualifier = getQualifier(DeviceAssignmentRecordType.Measurement, time);

		// Create measurements object and marshal to JSON.
		DeviceMeasurements measurements =
				SiteWherePersistence.deviceMeasurementsCreateLogic(request, assignment);
		byte[] json = MarshalUtils.marshalJson(measurements);

		// Create device measurements record. Fire and forget so errors only hit callback.
		PutRequest put =
				new PutRequest(ISiteWhereHBase.EVENTS_TABLE_NAME, rowkey, ISiteWhereHBase.FAMILY_ID,
						qualifier, json);
		hbase.getClient().put(put).addErrback(new PutFailedCallback());

		return measurements;
	}

	/**
	 * List measurements associated with an assignment based on the given criteria.
	 * 
	 * @param hbase
	 * @param assnToken
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	public static SearchResults<IDeviceMeasurements> listDeviceMeasurements(SiteWhereHBaseClient hbase,
			String assnToken, IDateRangeSearchCriteria criteria) throws SiteWhereException {
		Pager<byte[]> matches =
				getEventRowsForAssignment(hbase, assnToken, DeviceAssignmentRecordType.Measurement, criteria);
		return convertMatches(matches, DeviceMeasurements.class);
	}

	/**
	 * List device measurements associated with a site.
	 * 
	 * @param hbase
	 * @param siteToken
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	public static SearchResults<IDeviceMeasurements> listDeviceMeasurementsForSite(
			SiteWhereHBaseClient hbase, String siteToken, IDateRangeSearchCriteria criteria)
			throws SiteWhereException {
		Pager<byte[]> matches =
				getEventRowsForSite(hbase, siteToken, DeviceAssignmentRecordType.Measurement, criteria);
		return convertMatches(matches, DeviceMeasurements.class);
	}

	/**
	 * Create a new device location entry for an assignment.
	 * 
	 * @param hbase
	 * @param assignment
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static IDeviceLocation createDeviceLocation(SiteWhereHBaseClient hbase,
			IDeviceAssignment assignment, IDeviceLocationCreateRequest request) throws SiteWhereException {
		long time = getEventTime(request);
		byte[] assnKey = IdManager.getInstance().getAssignmentKeys().getValue(assignment.getToken());
		if (assnKey == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidDeviceAssignmentToken, ErrorLevel.ERROR);
		}
		byte[] rowkey = getRowKey(assnKey, time);
		byte[] qualifier = getQualifier(DeviceAssignmentRecordType.Location, time);

		DeviceLocation location = SiteWherePersistence.deviceLocationCreateLogic(assignment, request);
		byte[] json = MarshalUtils.marshalJson(location);

		// Create device location record. Fire and forget so errors only hit callback.
		PutRequest put =
				new PutRequest(ISiteWhereHBase.EVENTS_TABLE_NAME, rowkey, ISiteWhereHBase.FAMILY_ID,
						qualifier, json);
		hbase.getClient().put(put).addErrback(new PutFailedCallback());

		return location;
	}

	/**
	 * List locations associated with an assignment based on the given criteria.
	 * 
	 * @param hbase
	 * @param assnToken
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	public static SearchResults<IDeviceLocation> listDeviceLocations(SiteWhereHBaseClient hbase,
			String assnToken, IDateRangeSearchCriteria criteria) throws SiteWhereException {
		Pager<byte[]> matches =
				getEventRowsForAssignment(hbase, assnToken, DeviceAssignmentRecordType.Location, criteria);
		return convertMatches(matches, DeviceLocation.class);
	}

	/**
	 * List device locations associated with a site.
	 * 
	 * @param hbase
	 * @param siteToken
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	public static SearchResults<IDeviceLocation> listDeviceLocationsForSite(SiteWhereHBaseClient hbase,
			String siteToken, IDateRangeSearchCriteria criteria) throws SiteWhereException {
		Pager<byte[]> matches =
				getEventRowsForSite(hbase, siteToken, DeviceAssignmentRecordType.Location, criteria);
		return convertMatches(matches, DeviceLocation.class);
	}

	/**
	 * Create a new device alert entry for an assignment.
	 * 
	 * @param hbase
	 * @param assignment
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static IDeviceAlert createDeviceAlert(SiteWhereHBaseClient hbase, IDeviceAssignment assignment,
			IDeviceAlertCreateRequest request) throws SiteWhereException {
		long time = getEventTime(request);
		byte[] assnKey = IdManager.getInstance().getAssignmentKeys().getValue(assignment.getToken());
		if (assnKey == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidDeviceAssignmentToken, ErrorLevel.ERROR);
		}
		byte[] rowkey = getRowKey(assnKey, time);
		byte[] qualifier = getQualifier(DeviceAssignmentRecordType.Alert, time);

		// Create alert and marshal to JSON.
		DeviceAlert alert = SiteWherePersistence.deviceAlertCreateLogic(assignment, request);
		byte[] json = MarshalUtils.marshalJson(alert);

		// Create device alert record. Fire and forget so errors only hit callback.
		PutRequest put =
				new PutRequest(ISiteWhereHBase.EVENTS_TABLE_NAME, rowkey, ISiteWhereHBase.FAMILY_ID,
						qualifier, json);
		hbase.getClient().put(put).addErrback(new PutFailedCallback());

		return alert;
	}

	/**
	 * List alerts associated with an assignment based on the given criteria.
	 * 
	 * @param hbase
	 * @param assnToken
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	public static SearchResults<IDeviceAlert> listDeviceAlerts(SiteWhereHBaseClient hbase, String assnToken,
			IDateRangeSearchCriteria criteria) throws SiteWhereException {
		Pager<byte[]> matches =
				getEventRowsForAssignment(hbase, assnToken, DeviceAssignmentRecordType.Alert, criteria);
		return convertMatches(matches, DeviceAlert.class);
	}

	/**
	 * List device alerts associated with a site.
	 * 
	 * @param hbase
	 * @param siteToken
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	public static SearchResults<IDeviceAlert> listDeviceAlertsForSite(SiteWhereHBaseClient hbase,
			String siteToken, IDateRangeSearchCriteria criteria) throws SiteWhereException {
		Pager<byte[]> matches =
				getEventRowsForSite(hbase, siteToken, DeviceAssignmentRecordType.Alert, criteria);
		return convertMatches(matches, DeviceAlert.class);
	}

	/**
	 * Find all event rows associated with a device assignment and return cells that match
	 * the search criteria.
	 * 
	 * @param hbase
	 * @param assnToken
	 * @param eventType
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	protected static Pager<byte[]> getEventRowsForAssignment(SiteWhereHBaseClient hbase, String assnToken,
			DeviceAssignmentRecordType eventType, IDateRangeSearchCriteria criteria)
			throws SiteWhereException {
		byte[] assnKey = IdManager.getInstance().getAssignmentKeys().getValue(assnToken);
		if (assnKey == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidDeviceAssignmentToken, ErrorLevel.ERROR);
		}
		Scanner scanner = hbase.getClient().newScanner(ISiteWhereHBase.EVENTS_TABLE_NAME);
		scanner.setFamily(ISiteWhereHBase.FAMILY_ID);

		// Note: Because time values are inverted, start and end keys are reversed.
		byte[] startKey = null, endKey = null;
		if (criteria.getStartDate() != null) {
			startKey = getRowKey(assnKey, criteria.getEndDate().getTime());
		} else {
			startKey = getAbsoluteStartKey(assnKey);
		}
		if (criteria.getEndDate() != null) {
			endKey = getRowKey(assnKey, criteria.getStartDate().getTime());
		} else {
			endKey = getAbsoluteEndKey(assnKey);
		}
		scanner.setStartKey(startKey);
		scanner.setStopKey(endKey);

		Pager<byte[]> pager = new Pager<byte[]>(criteria);
		ArrayList<ArrayList<KeyValue>> current = new ArrayList<ArrayList<KeyValue>>();
		try {
			while ((current = scanner.nextRows().joinUninterruptibly()) != null) {
				for (ArrayList<KeyValue> row : current) {
					for (KeyValue column : row) {
						byte[] qual = column.qualifier();
						if ((qual.length > 3) && (qual[3] == eventType.getType())) {
							Date eventDate = getDateForEventKeyValue(column);
							if ((criteria.getStartDate() != null)
									&& (eventDate.before(criteria.getStartDate()))) {
								continue;
							}
							if ((criteria.getEndDate() != null) && (eventDate.after(criteria.getEndDate()))) {
								continue;
							}
							pager.process(column.value());
						}
					}
				}
			}
			scanner.close().joinUninterruptibly();
			return pager;
		} catch (Exception e) {
			throw new SiteWhereException("Error retrieving event rows.", e);
		}
	}

	/**
	 * Decodes the event date encoded in the rowkey and qualifier for events.
	 * 
	 * @param kv
	 * @return
	 */
	protected static Date getDateForEventKeyValue(KeyValue kv) {
		byte[] key = kv.key();
		byte[] work = new byte[8];
		work[4] = (byte) ~key[7];
		work[5] = (byte) ~key[8];
		work[6] = (byte) ~key[9];
		work[7] = (byte) ~key[10];
		long base = Bytes.getLong(work);
		byte[] qual = kv.qualifier();
		work = new byte[8];
		work[5] = (byte) ~qual[0];
		work[6] = (byte) ~qual[1];
		work[7] = (byte) ~qual[2];
		long offset = Bytes.getLong(work);
		return new Date((base + offset) * 1000);
	}

	/**
	 * Find all event rows associated with a site and return values that match the search
	 * criteria. TODO: This is not optimized at all and will take forever in cases where
	 * there are ton of assignments and events. It has to go through every record
	 * associated with the site. It works for now though.
	 * 
	 * @param hbase
	 * @param siteToken
	 * @param eventType
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	protected static Pager<byte[]> getEventRowsForSite(SiteWhereHBaseClient hbase, String siteToken,
			DeviceAssignmentRecordType eventType, IDateRangeSearchCriteria criteria)
			throws SiteWhereException {
		Long siteId = IdManager.getInstance().getSiteKeys().getValue(siteToken);
		if (siteId == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidSiteToken, ErrorLevel.ERROR);
		}
		byte[] startPrefix = HBaseSite.getAssignmentRowKey(siteId);
		byte[] afterPrefix = HBaseSite.getAfterAssignmentRowKey(siteId);

		Scanner scanner = hbase.getClient().newScanner(ISiteWhereHBase.EVENTS_TABLE_NAME);
		scanner.setFamily(ISiteWhereHBase.FAMILY_ID);
		scanner.setStartKey(startPrefix);
		scanner.setStopKey(afterPrefix);

		Pager<byte[]> pager = new Pager<byte[]>(criteria);
		ArrayList<ArrayList<KeyValue>> current = new ArrayList<ArrayList<KeyValue>>();
		try {
			while ((current = scanner.nextRows().joinUninterruptibly()) != null) {
				for (ArrayList<KeyValue> row : current) {
					for (KeyValue column : row) {
						byte[] key = column.key();
						if (key.length > 7) {
							byte[] qual = column.qualifier();
							if ((qual.length > 3) && (qual[3] == eventType.getType())) {
								Date eventDate = getDateForEventKeyValue(column);
								if ((criteria.getStartDate() != null)
										&& (eventDate.before(criteria.getStartDate()))) {
									continue;
								}
								if ((criteria.getEndDate() != null)
										&& (eventDate.after(criteria.getEndDate()))) {
									continue;
								}
								pager.process(column.value());
							}
						}
					}
				}
			}
			scanner.close().joinUninterruptibly();
			return pager;
		} catch (Exception e) {
			throw new SiteWhereException("Error retrieving event rows.", e);
		}
	}

	/**
	 * Converts matching rows to {@link SearchResults} for web service response.
	 * 
	 * @param matches
	 * @param jsonType
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected static <I extends IDeviceEvent, D extends DeviceEvent> SearchResults<I> convertMatches(
			Pager<byte[]> matches, Class<D> jsonType) {
		ObjectMapper mapper = new ObjectMapper();
		List<I> results = new ArrayList<I>();
		for (byte[] jsonBytes : matches.getResults()) {
			try {
				D event = mapper.readValue(jsonBytes, jsonType);
				results.add((I) event);
			} catch (Throwable e) {
				LOGGER.error("Unable to read JSON value into event object.", e);
			}
		}
		return new SearchResults<I>(results, matches.getTotal());
	}

	/**
	 * Gets the absolute first possible event key for cases where a start timestamp is not
	 * specified.
	 * 
	 * @param assnKey
	 * @return
	 */
	protected static byte[] getAbsoluteStartKey(byte[] assnKey) {
		ByteBuffer buffer = ByteBuffer.allocate(assnKey.length + 4);
		buffer.put(assnKey);
		buffer.put((byte) 0x00);
		buffer.put((byte) 0x00);
		buffer.put((byte) 0x00);
		buffer.put((byte) 0x00);
		return buffer.array();
	}

	/**
	 * Gets the absolute first possible event key for cases where a start timestamp is not
	 * specified.
	 * 
	 * @param assnKey
	 * @return
	 */
	protected static byte[] getAbsoluteEndKey(byte[] assnKey) {
		ByteBuffer buffer = ByteBuffer.allocate(assnKey.length + 4);
		buffer.put(assnKey);
		buffer.put((byte) 0xff);
		buffer.put((byte) 0xff);
		buffer.put((byte) 0xff);
		buffer.put((byte) 0xff);
		return buffer.array();
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
	public static byte[] getRowKey(byte[] assnKey, long time) throws SiteWhereException {
		time = time / 1000;
		long bucket = time - (time % BUCKET_INTERVAL);
		byte[] bucketBytes = Bytes.fromLong(bucket);
		ByteBuffer buffer = ByteBuffer.allocate(assnKey.length + 4);
		buffer.put(assnKey);
		buffer.put((byte) ~bucketBytes[4]);
		buffer.put((byte) ~bucketBytes[5]);
		buffer.put((byte) ~bucketBytes[6]);
		buffer.put((byte) ~bucketBytes[7]);
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
		time = time / 1000;
		long offset = time % BUCKET_INTERVAL;
		byte[] offsetBytes = Bytes.fromLong(offset);
		ByteBuffer buffer = ByteBuffer.allocate(4);
		buffer.put((byte) ~offsetBytes[5]);
		buffer.put((byte) ~offsetBytes[6]);
		buffer.put((byte) ~offsetBytes[7]);
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