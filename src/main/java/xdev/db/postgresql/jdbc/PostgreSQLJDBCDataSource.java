package xdev.db.postgresql.jdbc;

import xdev.db.DBException;
import xdev.db.jdbc.JDBCDataSource;


public class PostgreSQLJDBCDataSource extends
		JDBCDataSource<PostgreSQLJDBCDataSource, PostgreSQLDbms>
{
	public PostgreSQLJDBCDataSource()
	{
		super(new PostgreSQLDbms());
	}
	
	
	@Override
	public Parameter[] getDefaultParameters()
	{
		return new Parameter[]{HOST.clone(),PORT.clone(5432),USERNAME.clone("postgres"),
				PASSWORD.clone(),CATALOG.clone(),URL_EXTENSION.clone(),
				IS_SERVER_DATASOURCE.clone(),SERVER_URL.clone(),AUTH_KEY.clone()};
	}
	
	
	@Override
	protected PostgreSQLConnectionInformation getConnectionInformation()
	{
		return new PostgreSQLConnectionInformation(getHost(),getPort(),getUserName(),getPassword()
				.getPlainText(),getCatalog(),getUrlExtension(),getDbmsAdaptor());
	}
	
	@Override
	public PostgreSQLJDBCConnection openConnectionImpl() throws DBException
	{
		return new PostgreSQLJDBCConnection(this);
	}

	@Override
	public PostgreSQLJDBCMetaData getMetaData() throws DBException
	{
		return new PostgreSQLJDBCMetaData(this);
	}
	
	@Override
	public boolean canExport()
	{
		return false;
	}
}
