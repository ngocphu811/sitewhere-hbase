/*
 * HBaseGrantedAuthority.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.hbase.user;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

import com.sitewhere.core.SiteWherePersistence;
import com.sitewhere.hbase.ISiteWhereHBase;
import com.sitewhere.hbase.SiteWhereHBaseClient;
import com.sitewhere.hbase.common.MarshalUtils;
import com.sitewhere.hbase.device.HBasePersistence;
import com.sitewhere.rest.model.user.GrantedAuthority;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.SiteWhereSystemException;
import com.sitewhere.spi.error.ErrorCode;
import com.sitewhere.spi.error.ErrorLevel;
import com.sitewhere.spi.user.IGrantedAuthority;
import com.sitewhere.spi.user.IGrantedAuthoritySearchCriteria;
import com.sitewhere.spi.user.request.IGrantedAuthorityCreateRequest;

/**
 * HBase specifics for dealing with SiteWhere granted authorities.
 * 
 * @author Derek
 */
public class HBaseGrantedAuthority {

	/** Static logger instance */
	private static Logger LOGGER = Logger.getLogger(HBaseGrantedAuthority.class);

	/**
	 * Create a new granted authority.
	 * 
	 * @param hbase
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static GrantedAuthority createGrantedAuthority(SiteWhereHBaseClient hbase,
			IGrantedAuthorityCreateRequest request) throws SiteWhereException {
		GrantedAuthority existing = getGrantedAuthorityByName(hbase, request.getAuthority());
		if (existing != null) {
			throw new SiteWhereSystemException(ErrorCode.DuplicateAuthority, ErrorLevel.ERROR,
					HttpServletResponse.SC_CONFLICT);
		}

		// Create the new granted authority and store it.
		GrantedAuthority auth = SiteWherePersistence.grantedAuthorityCreateLogic(request);
		byte[] primary = getGrantedAuthorityRowKey(request.getAuthority());
		byte[] json = MarshalUtils.marshalJson(auth);

		PutRequest put = new PutRequest(ISiteWhereHBase.USERS_TABLE_NAME, primary, ISiteWhereHBase.FAMILY_ID,
				ISiteWhereHBase.JSON_CONTENT, json);
		HBasePersistence.syncPut(hbase, put, "Unable to set JSON for granted authority.");

		return auth;
	}

	/**
	 * Get a granted authority by unique name.
	 * 
	 * @param hbase
	 * @param name
	 * @return
	 * @throws SiteWhereException
	 */
	public static GrantedAuthority getGrantedAuthorityByName(SiteWhereHBaseClient hbase, String name)
			throws SiteWhereException {
		byte[] rowkey = getGrantedAuthorityRowKey(name);
		GetRequest request = new GetRequest(ISiteWhereHBase.USERS_TABLE_NAME, rowkey).family(
				ISiteWhereHBase.FAMILY_ID).qualifier(ISiteWhereHBase.JSON_CONTENT);
		ArrayList<KeyValue> results = HBasePersistence.syncGet(hbase, request,
				"Unable to load granted authority by name.");
		if (results.size() != 1) {
			throw new SiteWhereException("Expected one JSON entry for granted authority and found: "
					+ results.size());
		}
		byte[] json = results.get(0).value();
		return MarshalUtils.unmarshalJson(json, GrantedAuthority.class);
	}

	/**
	 * List granted authorities that match the given criteria.
	 * 
	 * @param hbase
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	public static List<IGrantedAuthority> listGrantedAuthorities(SiteWhereHBaseClient hbase,
			IGrantedAuthoritySearchCriteria criteria) throws SiteWhereException {
		Scanner scanner = hbase.getClient().newScanner(ISiteWhereHBase.USERS_TABLE_NAME);
		scanner.setStartKey(new byte[] { UserRecordType.GrantedAuthority.getType() });
		scanner.setStopKey(new byte[] { UserRecordType.End.getType() });
		try {
			ArrayList<IGrantedAuthority> matches = new ArrayList<IGrantedAuthority>();
			ArrayList<ArrayList<KeyValue>> results;
			while ((results = scanner.nextRows().joinUninterruptibly()) != null) {
				for (ArrayList<KeyValue> row : results) {
					boolean shouldAdd = true;
					KeyValue jsonColumn = null;
					for (KeyValue column : row) {
						byte[] qualifier = column.qualifier();
						if (Bytes.equals(ISiteWhereHBase.JSON_CONTENT, qualifier)) {
							jsonColumn = column;
						}
					}
					if ((shouldAdd) && (jsonColumn != null)) {
						matches.add(MarshalUtils.unmarshalJson(jsonColumn.value(), GrantedAuthority.class));
					}
				}
			}
			return matches;
		} catch (Exception e) {
			throw new SiteWhereException("Error scanning results for listing granted authorities.", e);
		} finally {
			try {
				scanner.close().joinUninterruptibly();
			} catch (Exception e) {
				LOGGER.error("Error shutting down scanner for listing granted authorities.", e);
			}
		}
	}

	/**
	 * Get row key for a granted authority.
	 * 
	 * @param username
	 * @return
	 */
	public static byte[] getGrantedAuthorityRowKey(String name) {
		byte[] gaBytes = Bytes.UTF8(name);
		ByteBuffer buffer = ByteBuffer.allocate(1 + gaBytes.length);
		buffer.put(UserRecordType.GrantedAuthority.getType());
		buffer.put(gaBytes);
		return buffer.array();
	}
}