package xdev.db.postgresql.jdbc;

/*-
 * #%L
 * SqlEngine Database Adapter PostgreSQL
 * %%
 * Copyright (C) 2003 - 2021 XDEV Software
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-3.0.html>.
 * #L%
 */

import com.xdev.jadoth.sqlengine.dbms.standard.StandardRetrospectionAccessor;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineException;
import com.xdev.jadoth.sqlengine.internal.tables.SqlIndex;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;


public class PostgreSQLRetrospectionAccessor extends StandardRetrospectionAccessor<PostgreSQLDbms>
{
	private static final String RETROSPECTION_NOT_IMPLEMENTED_YET = "Retrospection not implemented yet!";


	public PostgreSQLRetrospectionAccessor(final PostgreSQLDbms dbmsadaptor)
	{
		super(dbmsadaptor);
	}
	
	
	@Override
	public String createSelect_INFORMATION_SCHEMA_COLUMNS(SqlTableIdentity table)
	{
		throw new RuntimeException(RETROSPECTION_NOT_IMPLEMENTED_YET);
	}
	
	
	@Override
	public String createSelect_INFORMATION_SCHEMA_INDICES(SqlTableIdentity table)
	{
		throw new RuntimeException(RETROSPECTION_NOT_IMPLEMENTED_YET);
	}
	
	
	@Override
	public SqlIndex[] loadIndices(SqlTableIdentity table) throws SQLEngineException
	{
		throw new RuntimeException(RETROSPECTION_NOT_IMPLEMENTED_YET);
	}
}
