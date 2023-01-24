package xdev.db.postgresql.jdbc;
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
