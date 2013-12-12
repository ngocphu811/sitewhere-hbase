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
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;

import com.sitewhere.spi.SiteWhereException;

/**
 * Holds classes used to access HBase via client interfaces.
 * 
 * @author Derek
 */
public class SiteWhereHBaseClient implements InitializingBean {

	/** Static logger instance */
	private static final Logger LOGGER = Logger.getLogger(SiteWhereHBaseClient.class);

	/** Zookeeper quorum */
	private String quorum;

	/** HBase configuration */
	private Configuration configuration;

	/** HBase connection */
	private HConnection connection;

	/** Standard admin interface */
	private HBaseAdmin admin;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		try {
			configuration = HBaseConfiguration.create();
			configuration.set("hbase.zookeeper.quorum", quorum);
			this.admin = new HBaseAdmin(configuration);
			this.connection = HConnectionManager.createConnection(configuration);
		} catch (Exception e) {
			throw new SiteWhereException(e);
		}
	}

	/**
	 * Stop all connectivity. TODO: Where does this eventually get called?
	 */
	public void stop() {
		if (getAdmin() != null) {
			try {
				getAdmin().shutdown();
			} catch (IOException e) {
				LOGGER.error("HBaseAdmin did not shut down cleanly.", e);
			}
			try {
				getConnection().close();
			} catch (IOException e) {
				LOGGER.error("HConnection did not close cleanly.", e);
			}
		}
	}

	public HBaseAdmin getAdmin() {
		return admin;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public HConnection getConnection() {
		return connection;
	}

	public String getQuorum() {
		return quorum;
	}

	public void setQuorum(String quorum) {
		this.quorum = quorum;
	}
}