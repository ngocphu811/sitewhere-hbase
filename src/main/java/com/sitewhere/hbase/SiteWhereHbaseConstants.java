/*
 * SiteWhereColumnFamily.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.hbase;

import org.hbase.async.Bytes;

/**
 * Constants associated with the SiteWhere column family.
 * 
 * @author Derek
 */
public interface SiteWhereHbaseConstants {

	/** SiteWhere family id */
	public static final byte[] FAMILY_ID = Bytes.UTF8("s");

	/** Sites table name */
	public static final byte[] UID_TABLE_NAME = Bytes.UTF8("sw-uids");

	/** Sites table name */
	public static final byte[] SITES_TABLE_NAME = Bytes.UTF8("sw-sites");

	/** Devices table name */
	public static final byte[] DEVICES_TABLE_NAME = Bytes.UTF8("sw-devices");

	/** Assignments table name */
	public static final byte[] ASSIGNMENTS_TABLE_NAME = Bytes.UTF8("sw-assignments");

	/** Measurements table name */
	public static final byte[] MEASUREMENTS_TABLE_NAME = Bytes.UTF8("sw-measurements");

	/** Locations table name */
	public static final byte[] LOCATIONS_TABLE_NAME = Bytes.UTF8("sw-locations");

	/** Alerts table name */
	public static final byte[] ALERTS_TABLE_NAME = Bytes.UTF8("sw-alerts");

	/** Zones table name */
	public static final byte[] ZONES_TABLE_NAME = Bytes.UTF8("sw-zones");

	/** Users table name */
	public static final byte[] USERS_TABLE_NAME = Bytes.UTF8("sw-users");

	/** Authorities table name */
	public static final byte[] AUTHORITIES_TABLE_NAME = Bytes.UTF8("sw-authorities");
}