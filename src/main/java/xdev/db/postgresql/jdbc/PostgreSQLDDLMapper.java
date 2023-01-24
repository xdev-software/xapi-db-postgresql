package xdev.db.postgresql.jdbc;

import com.xdev.jadoth.sqlengine.dbms.standard.StandardDDLMapper;


public class PostgreSQLDDLMapper extends StandardDDLMapper<PostgreSQLDbms>
{
	
	public PostgreSQLDDLMapper(PostgreSQLDbms dbmsAdaptor)
	{
		super(dbmsAdaptor);
	}
	
}
