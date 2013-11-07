/*
 * HBaseConnectivity.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import org.hbase.async.HBaseClient;

import com.sitewhere.spi.SiteWhereException;

/**
 * Holds classes used to access HBase via client interfaces.
 * 
 * @author Derek
 */
public class HBaseConnectivity {

	/** Static logger instance */
	private static final Logger LOGGER = Logger.getLogger(HBaseConnectivity.class);

	/** Zookeeper quorum */
	String quorum;

	/** Singleton HBase client instance */
	private HBaseClient client;

	/** Standard admin interface */
	private HBaseAdmin admin;

	/**
	 * Start connectivity.
	 * 
	 * @param quorum
	 * @throws SiteWhereException
	 */
	public void start(String quorum) throws SiteWhereException {
		setClient(new HBaseClient(quorum));
		try {
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", quorum);
			setAdmin(new HBaseAdmin(config));
		} catch (Exception e) {
			throw new SiteWhereException(e);
		}
	}

	/**
	 * Stop all connectivity.
	 */
	public void stop() {
		if (getClient() != null) {
			try {
				getClient().shutdown().joinUninterruptibly();
			} catch (Exception e) {
				LOGGER.error("Async HBase client did not shut down cleanly.", e);
			}
		}
		if (getAdmin() != null) {
			try {
				getAdmin().shutdown();
			} catch (IOException e) {
				LOGGER.error("HBaseAdmin did not shut down cleanly.", e);
			}
		}
	}

	public HBaseClient getClient() {
		return client;
	}

	public void setClient(HBaseClient client) {
		this.client = client;
	}

	public HBaseAdmin getAdmin() {
		return admin;
	}

	public void setAdmin(HBaseAdmin admin) {
		this.admin = admin;
	}

}