package xdev.db.postgresql.jdbc;

import com.xdev.jadoth.sqlengine.dbms.DbmsSyntax;


public class PostgreSQLSyntax extends DbmsSyntax.Implementation<PostgreSQLDbms>
{
	protected PostgreSQLSyntax()
	{
		super(wordSet(),wordSet());
	}
	
}
