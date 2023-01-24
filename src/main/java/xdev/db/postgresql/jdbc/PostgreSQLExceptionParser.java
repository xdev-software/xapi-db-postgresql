package xdev.db.postgresql.jdbc;

import java.sql.SQLException;

import com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineException;

public class PostgreSQLExceptionParser implements SQLExceptionParser
{
	/**
	 * @param e
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser#parseSQLException(java.sql.SQLException)
	 */
	@Override
	public SQLEngineException parseSQLException(SQLException e)
	{
		return new SQLEngineException(e);
	}
}
