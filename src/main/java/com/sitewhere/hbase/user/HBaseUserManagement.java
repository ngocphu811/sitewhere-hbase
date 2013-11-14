/*
 * HBaseUserManagement.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.hbase.user;

import java.util.List;

import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.user.IGrantedAuthority;
import com.sitewhere.spi.user.IGrantedAuthoritySearchCriteria;
import com.sitewhere.spi.user.IUser;
import com.sitewhere.spi.user.IUserManagement;
import com.sitewhere.spi.user.IUserSearchCriteria;
import com.sitewhere.spi.user.request.IGrantedAuthorityCreateRequest;
import com.sitewhere.spi.user.request.IUserCreateRequest;

/**
 * HBase implementation of SiteWhere user management.
 * 
 * @author Derek
 */
public class HBaseUserManagement implements IUserManagement {

	@Override
	public void start() throws SiteWhereException {
		// TODO Auto-generated method stub

	}

	@Override
	public void stop() throws SiteWhereException {
		// TODO Auto-generated method stub

	}

	@Override
	public IUser createUser(IUserCreateRequest request) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IUser authenticate(String username, String password) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IUser updateUser(String username, IUserCreateRequest request) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IUser getUserByUsername(String username) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<IGrantedAuthority> getGrantedAuthorities(String username) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<IGrantedAuthority> addGrantedAuthorities(String username, List<String> authorities)
			throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<IGrantedAuthority> removeGrantedAuthorities(String username, List<String> authorities)
			throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<IUser> listUsers(IUserSearchCriteria criteria) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IUser deleteUser(String username, boolean force) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGrantedAuthority createGrantedAuthority(IGrantedAuthorityCreateRequest request)
			throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGrantedAuthority getGrantedAuthorityByName(String name) throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGrantedAuthority updateGrantedAuthority(String name, IGrantedAuthorityCreateRequest request)
			throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<IGrantedAuthority> listGrantedAuthorities(IGrantedAuthoritySearchCriteria criteria)
			throws SiteWhereException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteGrantedAuthority(String authority) throws SiteWhereException {
		// TODO Auto-generated method stub

	}

}
