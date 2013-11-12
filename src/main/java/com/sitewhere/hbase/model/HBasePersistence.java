/*
 * HBasePersistence.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.hbase.model;

import java.util.ArrayList;
import java.util.Date;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import com.sitewhere.hbase.HBaseConnectivity;
import com.sitewhere.rest.model.common.MetadataProviderEntity;
import com.sitewhere.security.LoginManager;
import com.sitewhere.spi.SiteWhereException;

/**
 * Common persistence operations.
 * 
 * @author Derek
 */
public class HBasePersistence {

	/**
	 * Initialize entity fields.
	 * 
	 * @param entity
	 * @throws SiteWhereException
	 */
	public static void initializeEntityMetadata(MetadataProviderEntity entity) throws SiteWhereException {
		entity.setCreatedDate(new Date());
		entity.setCreatedBy(LoginManager.getCurrentlyLoggedInUser().getUsername());
		entity.setDeleted(false);
	}

	/**
	 * Send a synchronous put to the server.
	 * 
	 * @param hbase
	 * @param request
	 * @param errorMessage
	 * @return
	 * @throws SiteWhereException
	 */
	public static Object syncPut(HBaseConnectivity hbase, PutRequest request, String errorMessage)
			throws SiteWhereException {
		try {
			request.setBufferable(false);
			return hbase.getClient().put(request).joinUninterruptibly();
		} catch (Exception e) {
			throw new SiteWhereException(errorMessage, e);
		}
	}

	/**
	 * Send a synchronous delete to the server.
	 * 
	 * @param hbase
	 * @param request
	 * @param errorMessage
	 * @return
	 * @throws SiteWhereException
	 */
	public static Object syncDelete(HBaseConnectivity hbase, DeleteRequest request, String errorMessage)
			throws SiteWhereException {
		try {
			request.setBufferable(false);
			return hbase.getClient().delete(request).joinUninterruptibly();
		} catch (Exception e) {
			throw new SiteWhereException(errorMessage, e);
		}
	}

	/**
	 * Send a synchronous get to the server.
	 * 
	 * @param hbase
	 * @param request
	 * @param errorMessage
	 * @return
	 * @throws SiteWhereException
	 */
	public static ArrayList<KeyValue> syncGet(HBaseConnectivity hbase, GetRequest request, String errorMessage)
			throws SiteWhereException {
		try {
			return hbase.getClient().get(request).joinUninterruptibly();
		} catch (Exception e) {
			throw new SiteWhereException(errorMessage, e);
		}
	}
}