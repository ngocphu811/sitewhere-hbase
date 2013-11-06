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

/**
 * Constants associated with the SiteWhere column family.
 * 
 * @author Derek
 */
public interface SiteWhereHbaseConstants {

	/** SiteWhere family id */
	public static final byte[] FAMILY_ID = "s".getBytes();

	/** Sites table name */
	public static final byte[] SITES_TABLE_NAME = "sites".getBytes();

	/** Devices table name */
	public static final byte[] DEVICES_TABLE_NAME = "devices".getBytes();

	/** Assignments table name */
	public static final byte[] ASSIGNMENTS_TABLE_NAME = "assignments".getBytes();

	/** Measurements table name */
	public static final byte[] MEASUREMENTS_TABLE_NAME = "measurements".getBytes();

	/** Locations table name */
	public static final byte[] LOCATIONS_TABLE_NAME = "locations".getBytes();

	/** Alerts table name */
	public static final byte[] ALERTS_TABLE_NAME = "alerts".getBytes();

	/** Zones table name */
	public static final byte[] ZONES_TABLE_NAME = "zones".getBytes();

	/** Users table name */
	public static final byte[] USERS_TABLE_NAME = "users".getBytes();

	/** Authorities table name */
	public static final byte[] AUTHORITIES_TABLE_NAME = "authorities".getBytes();
}