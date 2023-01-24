package xdev.db.postgresql.jdbc;

import java.sql.Connection;

import xdev.db.ConnectionInformation;


public class PostgreSQLConnectionInformation extends ConnectionInformation<PostgreSQLDbms>
{
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	public PostgreSQLConnectionInformation(final String host, final int port, final String user,
			final String password, final String database, final String urlExtension,
			final PostgreSQLDbms dbmsAdaptor)
	{
		super(host,port,user,password,database,urlExtension,dbmsAdaptor);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// getters //
	// ///////////////////
	
	/**
	 * Gets the database.
	 * 
	 * @return the database
	 */
	public String getDatabase()
	{
		return this.getCatalog();
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// setters //
	// ///////////////////
	
	/**
	 * Sets the database.
	 * 
	 * @param database
	 *            the database to set
	 */
	public void setDatabase(final String database)
	{
		setCatalog(database);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsConnectionInformation#createJdbcConnectionUrl(java.lang.String)
	 */
	@Override
	public String createJdbcConnectionUrl()
	{
		String url = "jdbc:postgresql://" + getHost() + ":" + getPort() + "/" + getCatalog();
		return appendUrlExtension(url);
	}
	
	
	/**
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsConnectionInformation#getJdbcDriverClassName()
	 */
	@Override
	public String getJdbcDriverClassName()
	{
		return "org.postgresql.Driver";
	}
	
	
	@Override
	public boolean isConnectionValid(Connection connection)
	{
		try
		{
			// Postgre JDBC 4 driver doesn't support isValid()
			return !connection.isClosed() && connection.createStatement().execute("SELECT 1");
		}
		catch(Throwable e)
		{
			// Because we just want to know if it is a valid connection,
			// there is no need for throwing an exception
			return false;
		}
	}
}
