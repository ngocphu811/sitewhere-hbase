/*
 * HbaseDeviceManagement.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.hbase;

import java.util.List;

import org.apache.log4j.Logger;

import com.sitewhere.hbase.common.SiteWhereTables;
import com.sitewhere.hbase.uid.UniqueIdType;
import com.sitewhere.hbase.uid.UuidCounterMap;
import com.sitewhere.rest.service.search.SearchResults;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.common.IDateRangeSearchCriteria;
import com.sitewhere.spi.common.IMetadataProvider;
import com.sitewhere.spi.common.ISearchCriteria;
import com.sitewhere.spi.device.DeviceAssignmentStatus;
import com.sitewhere.spi.device.IDevice;
import com.sitewhere.spi.device.IDeviceAlert;
import com.sitewhere.spi.device.IDeviceAssignment;
import com.sitewhere.spi.device.IDeviceEventBatch;
import com.sitewhere.spi.device.IDeviceEventBatchResponse;
import com.sitewhere.spi.device.IDeviceLocation;
import com.sitewhere.spi.device.IDeviceManagement;
import com.sitewhere.spi.device.IDeviceMeasurements;
import com.sitewhere.spi.device.ISite;
import com.sitewhere.spi.device.IZone;
import com.sitewhere.spi.device.request.IDeviceAlertCreateRequest;
import com.sitewhere.spi.device.request.IDeviceAssignmentCreateRequest;
import com.sitewhere.spi.device.request.IDeviceCreateRequest;
import com.sitewhere.spi.device.request.IDeviceLocationCreateRequest;
import com.sitewhere.spi.device.request.IDeviceMeasurementsCreateRequest;
import com.sitewhere.spi.device.request.ISiteCreateRequest;
import com.sitewhere.spi.device.request.IZoneCreateRequest;

/**
 * HBase implementation of SiteWhere device management.
 * 
 * @author Derek
 */
public class HBaseDeviceManagement implements IDeviceManagement {

	/** Static logger instance */
	private static final Logger LOGGER = Logger.getLogger(HBaseDeviceManagement.class);

	/** Used to communicate with HBase */
	private HBaseConnectivity hbase;

	/** Keeps up with unique keys related to sites */
	private UuidCounterMap siteKeys;

	/** Zookeeper quorum */
	private String quorum;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sitewhere.spi.ISiteWhereLifecycle#start()
	 */
	public void start() throws SiteWhereException {
		LOGGER.info("HBase device management starting...");
		this.hbase = new HBaseConnectivity();
		hbase.start(getQuorum());
		ensureTablesExist();
		loadUniqueIdCaches();
		LOGGER.info("HBase device management started.");
	}

	/**
	 * Load caches with unique mappings for various entities.
	 * 
	 * @throws SiteWhereException
	 */
	protected void loadUniqueIdCaches() throws SiteWhereException {
		siteKeys = new UuidCounterMap(hbase, UniqueIdType.SiteKey, UniqueIdType.SiteValue);
		siteKeys.refresh();
		String uuid = siteKeys.createUniqueId();
		LOGGER.info("Created site UUID: " + uuid);
	}

	/**
	 * Make sure that all SiteWhere tables exist, creating them if necessary.
	 * 
	 * @throws SiteWhereException
	 */
	protected void ensureTablesExist() throws SiteWhereException {
		SiteWhereTables.assureTable(hbase, SiteWhereHBaseConstants.UID_TABLE_NAME);
		SiteWhereTables.assureTable(hbase, SiteWhereHBaseConstants.SITES_TABLE_NAME);
		SiteWhereTables.assureTable(hbase, SiteWhereHBaseConstants.DEVICES_TABLE_NAME);
		SiteWhereTables.assureTable(hbase, SiteWhereHBaseConstants.ASSIGNMENTS_TABLE_NAME);
		SiteWhereTables.assureTable(hbase, SiteWhereHBaseConstants.ZONES_TABLE_NAME);
		SiteWhereTables.assureTable(hbase, SiteWhereHBaseConstants.MEASUREMENTS_TABLE_NAME);
		SiteWhereTables.assureTable(hbase, SiteWhereHBaseConstants.LOCATIONS_TABLE_NAME);
		SiteWhereTables.assureTable(hbase, SiteWhereHBaseConstants.ALERTS_TABLE_NAME);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sitewhere.spi.ISiteWhereLifecycle#stop()
	 */
	public void stop() throws SiteWhereException {
		hbase.stop();
	}

	@Override
	public IDevice createDevice(IDeviceCreateRequest device) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDevice getDeviceByHardwareId(String hardwareId) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDevice updateDevice(String hardwareId, IDeviceCreateRequest request) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDeviceAssignment getCurrentDeviceAssignment(IDevice device) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchResults<IDevice> listDevices(boolean includeDeleted, ISearchCriteria criteria)
			throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchResults<IDevice> listUnassignedDevices(ISearchCriteria criteria) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDevice deleteDevice(String hardwareId, boolean force) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDeviceAssignment createDeviceAssignment(IDeviceAssignmentCreateRequest request)
			throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDeviceAssignment getDeviceAssignmentByToken(String token) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDeviceAssignment deleteDeviceAssignment(String token, boolean force) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDevice getDeviceForAssignment(IDeviceAssignment assignment) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ISite getSiteForAssignment(IDeviceAssignment assignment) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDeviceAssignment updateDeviceAssignmentMetadata(String token, IMetadataProvider metadata)
			throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDeviceAssignment updateDeviceAssignmentStatus(String token, DeviceAssignmentStatus status)
			throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDeviceAssignment updateDeviceAssignmentLocation(String token, String locationId)
			throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDeviceEventBatchResponse addDeviceEventBatch(String assignmentToken, IDeviceEventBatch batch)
			throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDeviceAssignment endDeviceAssignment(String token) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchResults<IDeviceAssignment> getDeviceAssignmentHistory(String hardwareId,
			ISearchCriteria criteria) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchResults<IDeviceAssignment> getDeviceAssignmentsForSite(String siteToken,
			ISearchCriteria criteria) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchResults<IDeviceAssignment> getDeviceAssignmentsNear(double latitude, double longitude,
			double maxDistance, ISearchCriteria criteria) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDeviceMeasurements addDeviceMeasurements(IDeviceAssignment assignment,
			IDeviceMeasurementsCreateRequest measurements) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchResults<IDeviceMeasurements> listDeviceMeasurements(String siteToken,
			IDateRangeSearchCriteria criteria) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchResults<IDeviceMeasurements> listDeviceMeasurementsForSite(String siteToken,
			IDateRangeSearchCriteria criteria) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void associateAlertWithMeasurements(String alertId, String measurementsId)
			throws SiteWhereException {
		// TODO Auto-generated method stub

	}

	@Override
	public IDeviceLocation addDeviceLocation(IDeviceAssignment assignment,
			IDeviceLocationCreateRequest request) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchResults<IDeviceLocation> listDeviceLocations(String assignmentToken,
			IDateRangeSearchCriteria criteria) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchResults<IDeviceLocation> listDeviceLocationsForSite(String siteToken,
			IDateRangeSearchCriteria criteria) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchResults<IDeviceLocation> listDeviceLocations(List<String> assignmentTokens,
			IDateRangeSearchCriteria criteria) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDeviceLocation associateAlertWithLocation(String alertId, String locationId)
			throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDeviceAlert addDeviceAlert(IDeviceAssignment assignment, IDeviceAlertCreateRequest request)
			throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchResults<IDeviceAlert> listDeviceAlerts(String assignmentToken,
			IDateRangeSearchCriteria criteria) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchResults<IDeviceAlert> listDeviceAlertsForSite(String siteToken,
			IDateRangeSearchCriteria criteria) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ISite createSite(ISiteCreateRequest request) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ISite deleteSite(String siteToken, boolean force) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ISite updateSite(String siteToken, ISiteCreateRequest request) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ISite getSiteByToken(String token) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchResults<ISite> listSites(ISearchCriteria criteria) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IZone createZone(ISite site, IZoneCreateRequest request) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IZone updateZone(String token, IZoneCreateRequest request) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IZone getZone(String zoneToken) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchResults<IZone> listZones(String siteToken, ISearchCriteria criteria)
			throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IZone deleteZone(String zoneToken, boolean force) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	public String getQuorum() {
		return quorum;
	}

	public void setQuorum(String quorum) {
		this.quorum = quorum;
	}
}