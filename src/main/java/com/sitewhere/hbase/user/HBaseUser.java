/*
 * HBaseUser.java 
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
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

import com.sitewhere.core.SiteWherePersistence;
import com.sitewhere.hbase.ISiteWhereHBase;
import com.sitewhere.hbase.SiteWhereHBaseClient;
import com.sitewhere.hbase.common.MarshalUtils;
import com.sitewhere.hbase.device.HBasePersistence;
import com.sitewhere.rest.model.user.GrantedAuthoritySearchCriteria;
import com.sitewhere.rest.model.user.User;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.SiteWhereSystemException;
import com.sitewhere.spi.error.ErrorCode;
import com.sitewhere.spi.error.ErrorLevel;
import com.sitewhere.spi.user.IGrantedAuthority;
import com.sitewhere.spi.user.IUser;
import com.sitewhere.spi.user.IUserSearchCriteria;
import com.sitewhere.spi.user.request.IUserCreateRequest;

/**
 * HBase specifics for dealing with SiteWhere users.
 * 
 * @author Derek
 */
public class HBaseUser {

	/** Static logger instance */
	private static Logger LOGGER = Logger.getLogger(HBaseUser.class);

	/**
	 * Create a new device.
	 * 
	 * @param hbase
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static User createUser(SiteWhereHBaseClient hbase, IUserCreateRequest request)
			throws SiteWhereException {
		User existing = getUserByUsername(hbase, request.getUsername());
		if (existing != null) {
			throw new SiteWhereSystemException(ErrorCode.DuplicateUser, ErrorLevel.ERROR,
					HttpServletResponse.SC_CONFLICT);
		}

		// Create the new user and store it.
		User user = SiteWherePersistence.userCreateLogic(request);
		byte[] primary = getUserRowKey(request.getUsername());
		byte[] json = MarshalUtils.marshalJson(user);

		PutRequest put = new PutRequest(ISiteWhereHBase.USERS_TABLE_NAME, primary, ISiteWhereHBase.FAMILY_ID,
				ISiteWhereHBase.JSON_CONTENT, json);
		HBasePersistence.syncPut(hbase, put, "Unable to set JSON for user.");

		return user;
	}

	/**
	 * Update an existing user.
	 * 
	 * @param hbase
	 * @param username
	 * @param request
	 * @return
	 * @throws SiteWhereException
	 */
	public static User updateUser(SiteWhereHBaseClient hbase, String username, IUserCreateRequest request)
			throws SiteWhereException {
		User updated = getUserByUsername(hbase, username);
		if (updated == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidUsername, ErrorLevel.ERROR);
		}
		SiteWherePersistence.userUpdateLogic(request, updated);

		byte[] primary = getUserRowKey(username);
		byte[] json = MarshalUtils.marshalJson(updated);

		PutRequest put = new PutRequest(ISiteWhereHBase.USERS_TABLE_NAME, primary, ISiteWhereHBase.FAMILY_ID,
				ISiteWhereHBase.JSON_CONTENT, json);
		HBasePersistence.syncPut(hbase, put, "Unable to set JSON for user.");
		return updated;
	}

	/**
	 * Delete an existing user.
	 * 
	 * @param hbase
	 * @param username
	 * @param force
	 * @return
	 * @throws SiteWhereException
	 */
	public static User deleteUser(SiteWhereHBaseClient hbase, String username, boolean force)
			throws SiteWhereException {
		User existing = getUserByUsername(hbase, username);
		if (existing == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidUsername, ErrorLevel.ERROR);
		}
		existing.setDeleted(true);
		byte[] primary = getUserRowKey(username);
		if (force) {
			DeleteRequest delete = new DeleteRequest(ISiteWhereHBase.USERS_TABLE_NAME, primary);
			try {
				hbase.getClient().delete(delete).joinUninterruptibly();
			} catch (Exception e) {
				throw new SiteWhereException("Unable to delete user.", e);
			}
		} else {
			byte[] marker = { (byte) 0x01 };
			SiteWherePersistence.setUpdatedEntityMetadata(existing);
			byte[] updated = MarshalUtils.marshalJson(existing);
			byte[][] qualifiers = { ISiteWhereHBase.JSON_CONTENT, ISiteWhereHBase.DELETED };
			byte[][] values = { updated, marker };
			PutRequest put = new PutRequest(ISiteWhereHBase.USERS_TABLE_NAME, primary,
					ISiteWhereHBase.FAMILY_ID, qualifiers, values);
			HBasePersistence.syncPut(hbase, put, "Unable to set deleted flag for user.");
		}
		return existing;
	}

	/**
	 * Get a user by unique username. Returns null if not found.
	 * 
	 * @param hbase
	 * @param username
	 * @return
	 * @throws SiteWhereException
	 */
	public static User getUserByUsername(SiteWhereHBaseClient hbase, String username)
			throws SiteWhereException {
		byte[] rowkey = getUserRowKey(username);
		GetRequest request = new GetRequest(ISiteWhereHBase.USERS_TABLE_NAME, rowkey).family(
				ISiteWhereHBase.FAMILY_ID).qualifier(ISiteWhereHBase.JSON_CONTENT);
		ArrayList<KeyValue> results = HBasePersistence.syncGet(hbase, request,
				"Unable to load user by username.");
		if (results.size() == 0) {
			return null;
		}
		if (results.size() > 1) {
			throw new SiteWhereException("Expected one JSON entry for user and found: " + results.size());
		}
		byte[] json = results.get(0).value();
		return MarshalUtils.unmarshalJson(json, User.class);
	}

	/**
	 * Authenticate a username password combination.
	 * 
	 * @param hbase
	 * @param username
	 * @param password
	 * @return
	 * @throws SiteWhereException
	 */
	public static User authenticate(SiteWhereHBaseClient hbase, String username, String password)
			throws SiteWhereException {
		if (password == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidPassword, ErrorLevel.ERROR,
					HttpServletResponse.SC_BAD_REQUEST);
		}
		User existing = getUserByUsername(hbase, username);
		if (existing == null) {
			throw new SiteWhereSystemException(ErrorCode.InvalidUsername, ErrorLevel.ERROR,
					HttpServletResponse.SC_UNAUTHORIZED);
		}
		String inPassword = SiteWherePersistence.encodePassoword(password);
		if (!existing.getHashedPassword().equals(inPassword)) {
			throw new SiteWhereSystemException(ErrorCode.InvalidPassword, ErrorLevel.ERROR,
					HttpServletResponse.SC_UNAUTHORIZED);
		}

		// Update last login date.
		existing.setLastLogin(new Date());
		byte[] primary = getUserRowKey(username);
		byte[] json = MarshalUtils.marshalJson(existing);

		// Create primary device record.
		PutRequest put = new PutRequest(ISiteWhereHBase.USERS_TABLE_NAME, primary, ISiteWhereHBase.FAMILY_ID,
				ISiteWhereHBase.JSON_CONTENT, json);
		HBasePersistence.syncPut(hbase, put, "Unable to set JSON for user.");
		return existing;
	}

	/**
	 * List users that match certain criteria.
	 * 
	 * @param hbase
	 * @param criteria
	 * @return
	 * @throws SiteWhereException
	 */
	public static List<IUser> listUsers(SiteWhereHBaseClient hbase, IUserSearchCriteria criteria)
			throws SiteWhereException {
		Scanner scanner = hbase.getClient().newScanner(ISiteWhereHBase.USERS_TABLE_NAME);
		scanner.setStartKey(new byte[] { UserRecordType.User.getType() });
		scanner.setStopKey(new byte[] { UserRecordType.GrantedAuthority.getType() });
		try {
			ArrayList<IUser> matches = new ArrayList<IUser>();
			ArrayList<ArrayList<KeyValue>> results;
			while ((results = scanner.nextRows().joinUninterruptibly()) != null) {
				for (ArrayList<KeyValue> row : results) {
					boolean shouldAdd = true;
					KeyValue jsonColumn = null;
					for (KeyValue column : row) {
						byte[] qualifier = column.qualifier();
						if ((Bytes.equals(ISiteWhereHBase.DELETED, qualifier))
								&& (!criteria.isIncludeDeleted())) {
							shouldAdd = false;
						}
						if (Bytes.equals(ISiteWhereHBase.JSON_CONTENT, qualifier)) {
							jsonColumn = column;
						}
					}
					if ((shouldAdd) && (jsonColumn != null)) {
						matches.add(MarshalUtils.unmarshalJson(jsonColumn.value(), User.class));
					}
				}
			}
			return matches;
		} catch (Exception e) {
			throw new SiteWhereException("Error scanning results for listing users.", e);
		} finally {
			try {
				scanner.close().joinUninterruptibly();
			} catch (Exception e) {
				LOGGER.error("Error shutting down scanner for listing users.", e);
			}
		}
	}

	/**
	 * Get the list of granted authorities for a user.
	 * 
	 * @param hbase
	 * @param username
	 * @return
	 * @throws SiteWhereException
	 */
	public static List<IGrantedAuthority> getGrantedAuthorities(SiteWhereHBaseClient hbase, String username)
			throws SiteWhereException {
		IUser user = getUserByUsername(hbase, username);
		List<String> userAuths = user.getAuthorities();
		List<IGrantedAuthority> all = HBaseGrantedAuthority.listGrantedAuthorities(hbase,
				new GrantedAuthoritySearchCriteria());
		List<IGrantedAuthority> matched = new ArrayList<IGrantedAuthority>();
		for (IGrantedAuthority auth : all) {
			if (userAuths.contains(auth.getAuthority())) {
				matched.add(auth);
			}
		}
		return matched;
	}

	/**
	 * Get row key for a user.
	 * 
	 * @param username
	 * @return
	 */
	public static byte[] getUserRowKey(String username) {
		byte[] userBytes = Bytes.UTF8(username);
		ByteBuffer buffer = ByteBuffer.allocate(1 + userBytes.length);
		buffer.put(UserRecordType.User.getType());
		buffer.put(userBytes);
		return buffer.array();
	}
}
