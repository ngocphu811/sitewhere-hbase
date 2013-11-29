/*
 * HBaseSite.java 
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

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.log4j.Logger;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sitewhere.core.SiteWherePersistence;
import com.sitewhere.hbase.ISiteWhereHBase;
import com.sitewhere.hbase.SiteWhereHBaseClient;
import com.sitewhere.hbase.common.MarshalUtils;
import com.sitewhere.hbase.common.Pager;
import com.sitewhere.hbase.common.SiteWhereTables;
import com.sitewhere.hbase.uid.IdManager;
import com.sitewhere.rest.model.device.DeviceAssignment;
import com.sitewhere.rest.model.device.Site;
import com.sitewhere.rest.model.device.Zone;
import com.sitewhere.rest.model.search.SearchResults;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.SiteWhereSystemException;
import com.sitewhere.spi.device.IDeviceAssignment;
import com.sitewhere.spi.device.ISite;
import com.sitewhere.spi.device.IZone;
import com.sitewhere.spi.device.request.ISiteCreateRequest;
import com.sitewhere.spi.error.ErrorCode;
import com.sitewhere.spi.error.ErrorLevel;
import com.sitewhere.spi.search.ISearchCriteria;

/**
 * HBase specifics for dealing with SiteWhere sites.
 * 
 * @author Derek
 */
public class HBaseSite {

	/** Private logger instance */
	private static Logger LOGGER = Logger.getLogger(HBaseSite.class);

	/** Length of site identifier (subset of 8 byte long) */
	public static final int SITE_IDENTIFIER_LENGTH = 2;

	/** Column qualifier for zone counter */
	public static final byte[] ZONE_COUNTER = Bytes.UTF8("zonectr");

	/** Column qualifier for assignment counter */
	public static final byte[] ASSIGNMENT_COUNTER = Bytes.UTF8("assnctr");

	/** Regex for getting site rows */
	public static final String REGEX_SITE = "^.{2}$";

	/**
	 * Create a new site.
	 * 
	 * @param hbase
	 * @param uids
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static ISite createSite(SiteWhereHBaseClient hbase, ISiteCreateRequest request)
			throws SiteWhereException {
		String uuid = IdManager.getInstance().getSiteKeys().createUniqueId();
		Long value = IdManager.getInstance().getSiteKeys().getValue(uuid);
		byte[] primary = getPrimaryRowkey(value);

		// Use common logic so all backend implementations work the same.
		Site site = SiteWherePersistence.siteCreateLogic(request, uuid);

		// Create primary site record.
		byte[] json = MarshalUtils.marshalJson(site);
		byte[] maxLong = Bytes.fromLong(Long.MAX_VALUE);
		byte[][] qualifiers = { ISiteWhereHBase.JSON_CONTENT, ZONE_COUNTER, ASSIGNMENT_COUNTER };
		byte[][] values = { json, maxLong, maxLong };
		PutRequest put = new PutRequest(ISiteWhereHBase.SITES_TABLE_NAME, primary, ISiteWhereHBase.FAMILY_ID,
				qualifiers, values);
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
	public static Site getSiteByToken(SiteWhereHBaseClient hbase, String token) throws SiteWhereException {
		Long siteId = IdManager.getInstance().getSiteKeys().getValue(token);
		if (siteId == null) {
			return null;
		}
		byte[] primary = getPrimaryRowkey(siteId);
		GetRequest request = new GetRequest(ISiteWhereHBase.SITES_TABLE_NAME, primary).family(
				ISiteWhereHBase.FAMILY_ID).qualifier(ISiteWhereHBase.JSON_CONTENT);
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
	public static Site updateSite(SiteWhereHBaseClient hbase, String token, ISiteCreateRequest request)
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
		PutRequest put = new PutRequest(ISiteWhereHBase.SITES_TABLE_NAME, rowkey, ISiteWhereHBase.FAMILY_ID,
				ISiteWhereHBase.JSON_CONTENT, json);
		HBasePersistence.syncPut(hbase, put, "Unable to update site.");
		return updated;
	}

	/**
	 * List all sites that match the given criteria.
	 * 
	 * @param hbase
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	public static SearchResults<ISite> listSites(SiteWhereHBaseClient hbase, ISearchCriteria criteria)
			throws SiteWhereException {
		RegexStringComparator comparator = new RegexStringComparator(REGEX_SITE);
		Pager<byte[]> pager = getFilteredSiteRows(hbase, false, criteria, comparator, null, null);
		List<ISite> response = new ArrayList<ISite>();
		for (byte[] match : pager.getResults()) {
			response.add(MarshalUtils.unmarshalJson(match, Site.class));
		}
		return new SearchResults<ISite>(response, pager.getTotal());
	}

	/**
	 * List device assignments for a given site.
	 * 
	 * @param hbase
	 * @param siteToken
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	public static SearchResults<IDeviceAssignment> listDeviceAssignmentsForSite(SiteWhereHBaseClient hbase,
			String siteToken, ISearchCriteria criteria) throws SiteWhereException {
		Long siteId = IdManager.getInstance().getSiteKeys().getValue(siteToken);
		if (siteId == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidSiteToken, ErrorLevel.ERROR);
		}
		byte[] assnPrefix = getAssignmentRowKey(siteId);
		byte[] after = getAfterAssignmentRowKey(siteId);
		BinaryPrefixComparator comparator = new BinaryPrefixComparator(assnPrefix);
		Pager<byte[]> pager = getFilteredSiteRows(hbase, false, criteria, comparator, assnPrefix, after);
		List<IDeviceAssignment> response = new ArrayList<IDeviceAssignment>();
		for (byte[] match : pager.getResults()) {
			response.add(MarshalUtils.unmarshalJson(match, DeviceAssignment.class));
		}
		return new SearchResults<IDeviceAssignment>(response, pager.getTotal());
	}

	/**
	 * List zones for a given site.
	 * 
	 * @param hbase
	 * @param siteToken
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	public static SearchResults<IZone> listZonesForSite(SiteWhereHBaseClient hbase, String siteToken,
			ISearchCriteria criteria) throws SiteWhereException {
		Long siteId = IdManager.getInstance().getSiteKeys().getValue(siteToken);
		if (siteId == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidSiteToken, ErrorLevel.ERROR);
		}
		byte[] zonePrefix = getZoneRowKey(siteId);
		byte[] after = getAssignmentRowKey(siteId);
		BinaryPrefixComparator comparator = new BinaryPrefixComparator(zonePrefix);
		Pager<byte[]> pager = getFilteredSiteRows(hbase, false, criteria, comparator, zonePrefix, after);
		List<IZone> response = new ArrayList<IZone>();
		for (byte[] match : pager.getResults()) {
			response.add(MarshalUtils.unmarshalJson(match, Zone.class));
		}
		return new SearchResults<IZone>(response, pager.getTotal());
	}

	/**
	 * Get json associated with various rows in the site table based on regex filters.
	 * 
	 * @param hbase
	 * @param includeDeleted
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	public static Pager<byte[]> getFilteredSiteRows(SiteWhereHBaseClient hbase, boolean includeDeleted,
			ISearchCriteria criteria, WritableByteArrayComparable comparator, byte[] startRow, byte[] stopRow)
			throws SiteWhereException {
		HTable sites = SiteWhereTables.getHTable(hbase, ISiteWhereHBase.SITES_TABLE_NAME);
		ResultScanner scanner = null;
		try {
			RowFilter matcher = new RowFilter(CompareOp.EQUAL, comparator);
			Scan scan = new Scan();
			if (startRow != null) {
				scan.setStartRow(startRow);
			}
			if (stopRow != null) {
				scan.setStopRow(stopRow);
			}
			scan.setFilter(matcher);
			scanner = sites.getScanner(scan);

			Pager<byte[]> pager = new Pager<byte[]>(criteria);
			for (Result result : scanner) {
				boolean shouldAdd = true;
				byte[] json = null;
				for (org.apache.hadoop.hbase.KeyValue column : result.raw()) {
					byte[] qualifier = column.getQualifier();
					if ((Bytes.equals(ISiteWhereHBase.DELETED, qualifier)) && (!includeDeleted)) {
						shouldAdd = false;
					}
					if (Bytes.equals(ISiteWhereHBase.JSON_CONTENT, qualifier)) {
						json = column.getValue();
					}
				}
				if ((shouldAdd) && (json != null)) {
					pager.process(json);
				}
			}
			return pager;
		} catch (Exception e) {
			throw new SiteWhereException("Error scanning site rows.", e);
		} finally {
			try {
				if (scanner != null) {
					scanner.close();
				}
				sites.close();
			} catch (Throwable e) {
				LOGGER.error("Error on site search cleanup.", e);
			}
		}
	}

	/**
	 * Delete an existing site.
	 * 
	 * @param hbase
	 * @param token
	 * @param force
	 * @return
	 * @throws SiteWhereException
	 */
	public static Site deleteSite(SiteWhereHBaseClient hbase, String token, boolean force)
			throws SiteWhereException {
		Site existing = getSiteByToken(hbase, token);
		if (existing == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidSiteToken, ErrorLevel.ERROR);
		}
		existing.setDeleted(true);

		Long siteId = IdManager.getInstance().getSiteKeys().getValue(token);
		byte[] rowkey = getPrimaryRowkey(siteId);
		if (force) {
			IdManager.getInstance().getSiteKeys().delete(token);
			DeleteRequest delete = new DeleteRequest(ISiteWhereHBase.SITES_TABLE_NAME, rowkey);
			try {
				hbase.getClient().delete(delete).joinUninterruptibly();
			} catch (Exception e) {
				throw new SiteWhereException("Unable to delete site.", e);
			}
		} else {
			byte[] marker = { (byte) 0x01 };
			SiteWherePersistence.setUpdatedEntityMetadata(existing);
			byte[] updated = MarshalUtils.marshalJson(existing);
			byte[][] qualifiers = { ISiteWhereHBase.JSON_CONTENT, ISiteWhereHBase.DELETED };
			byte[][] values = { updated, marker };
			PutRequest put = new PutRequest(ISiteWhereHBase.SITES_TABLE_NAME, rowkey,
					ISiteWhereHBase.FAMILY_ID, qualifiers, values);
			HBasePersistence.syncPut(hbase, put, "Unable to set deleted flag for site.");
		}
		return existing;
	}

	/**
	 * Allocate the next zone id and return the new value. (Each id is less than the last)
	 * 
	 * @param hbase
	 * @param siteId
	 * @return
	 * @throws SiteWhereException
	 */
	public static Long allocateNextZoneId(SiteWhereHBaseClient hbase, Long siteId) throws SiteWhereException {
		byte[] primary = getPrimaryRowkey(siteId);
		AtomicIncrementRequest request = new AtomicIncrementRequest(ISiteWhereHBase.SITES_TABLE_NAME,
				primary, ISiteWhereHBase.FAMILY_ID, ZONE_COUNTER, -1);
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
	public static Long allocateNextAssignmentId(SiteWhereHBaseClient hbase, Long siteId)
			throws SiteWhereException {
		byte[] primary = getPrimaryRowkey(siteId);
		AtomicIncrementRequest request = new AtomicIncrementRequest(ISiteWhereHBase.SITES_TABLE_NAME,
				primary, ISiteWhereHBase.FAMILY_ID, ASSIGNMENT_COUNTER, -1);
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
