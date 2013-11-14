/*
 * HBasePersistence.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.hbase.device;

import java.util.ArrayList;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import com.sitewhere.hbase.SiteWhereHBaseClient;
import com.sitewhere.spi.SiteWhereException;

/**
 * Common persistence operations.
 * 
 * @author Derek
 */
public class HBasePersistence {

	/**
	 * Send a synchronous put to the server.
	 * 
	 * @param hbase
	 * @param request
	 * @param errorMessage
	 * @return
	 * @throws SiteWhereException
	 */
	public static Object syncPut(SiteWhereHBaseClient hbase, PutRequest request, String errorMessage)
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
	public static Object syncDelete(SiteWhereHBaseClient hbase, DeleteRequest request, String errorMessage)
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
	public static ArrayList<KeyValue> syncGet(SiteWhereHBaseClient hbase, GetRequest request, String errorMessage)
			throws SiteWhereException {
		try {
			return hbase.getClient().get(request).joinUninterruptibly();
		} catch (Exception e) {
			throw new SiteWhereException(errorMessage, e);
		}
	}
}