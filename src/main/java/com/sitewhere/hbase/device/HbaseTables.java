/*
 * HbaseSites.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.hbase.device;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import com.sitewhere.hbase.SiteWhereHbaseConstants;
import com.sitewhere.spi.SiteWhereException;

/**
 * Utility method for SiteWhere HBase tables.
 * 
 * @author Derek
 */
public class HbaseTables {

	/** Static logger instance */
	private static Logger LOGGER = Logger.getLogger(HbaseTables.class);

	/**
	 * Assure that the given table exists and create it if not.
	 * 
	 * @param config
	 * @param tableName
	 * @throws SiteWhereException
	 */
	public static void assureTable(HBaseAdmin admin, byte[] tableName) throws SiteWhereException {
		try {
			String tnameStr = new String(tableName);
			if (!admin.tableExists(tableName)) {
				LOGGER.info("Table '" + tnameStr + "' does not exist. Creating table...");
				HTableDescriptor table = new HTableDescriptor(tableName);
				HColumnDescriptor family = new HColumnDescriptor(SiteWhereHbaseConstants.FAMILY_ID);
				table.addFamily(family);
				admin.createTable(table);
				LOGGER.info("Table '" + tnameStr + "' created successfully.");
			} else {
				LOGGER.info("Table '" + tnameStr + "' verfied.");
			}
		} catch (IOException e) {
			throw new SiteWhereException(e);
		}
	}
}