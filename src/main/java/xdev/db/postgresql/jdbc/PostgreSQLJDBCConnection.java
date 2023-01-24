package xdev.db.postgresql.jdbc;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import xdev.db.DBException;
import xdev.db.jdbc.JDBCConnection;


public class PostgreSQLJDBCConnection extends
		JDBCConnection<PostgreSQLJDBCDataSource, PostgreSQLDbms>
{
	public PostgreSQLJDBCConnection(PostgreSQLJDBCDataSource dataSource)
	{
		super(dataSource);
	}
	
	
	@Override
	protected void setPreparedStatementParameter(PreparedStatement statement, Object parameter,
			int jdbcIndex) throws SQLException, DBException
	{
		boolean inTransaction = isInTransaction();
		if(!inTransaction && parameter instanceof Blob)
		{
			Blob blob = (Blob)parameter;
			statement.setBytes(jdbcIndex,blob.getBytes(1,(int)blob.length()));
		}
		else if(!inTransaction && parameter instanceof Clob)
		{
			Clob clob = (Clob)parameter;
			statement.setString(jdbcIndex,clob.getSubString(1,(int)clob.length()));
		}
		else
		{
			super.setPreparedStatementParameter(statement,parameter,jdbcIndex);
		}
	}
	
	
	@Override
	public void createTable(String tableName, String primaryKey, Map<String, String> columnMap,
			boolean isAutoIncrement, Map<String, String> foreignKeys) throws Exception
	{
		
		if(!columnMap.containsKey(primaryKey))
		{
			columnMap.put(primaryKey,"INTEGER"); //$NON-NLS-1$
		}
		
		StringBuffer createStatement = null;
		if(isAutoIncrement)
		{
			createStatement = new StringBuffer("CREATE TABLE IF NOT EXISTS \"" + tableName //$NON-NLS-1$
					+ "\"(\"" + primaryKey + "\" SERIAL NOT NULL,"); //$NON-NLS-1$ //$NON-NLS-2$
		}
		else
		{
			createStatement = new StringBuffer("CREATE TABLE IF NOT EXISTS \"" + tableName //$NON-NLS-1$
					+ "\"(\"" + primaryKey + "\" " + columnMap.get(primaryKey) + ","); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		}
		
		for(String keySet : columnMap.keySet())
		{
			if(!keySet.equals(primaryKey))
			{
				createStatement.append("\"" + keySet + "\" " + columnMap.get(keySet) + ","); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}
		}
		
		createStatement.append(" PRIMARY KEY (\"" + primaryKey + "\"))"); //$NON-NLS-1$ //$NON-NLS-2$
		
		if(log.isDebugEnabled())
		{
			log.debug("SQL Statement to create a table: " + createStatement.toString()); //$NON-NLS-1$
		}
		
		Connection connection = super.getConnection();
		Statement statement = connection.createStatement();
		try
		{
			statement.execute(createStatement.toString());
		}
		catch(Exception e)
		{
			throw e;
		}
		finally
		{
			statement.close();
			connection.close();
		}
	}
}
