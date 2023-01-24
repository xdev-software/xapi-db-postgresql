package xdev.db.postgresql.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;

import xdev.db.ColumnMetaData;
import xdev.db.DBException;
import xdev.db.DataType;
import xdev.db.Index;
import xdev.db.Index.IndexType;
import xdev.db.Result;
import xdev.db.StoredProcedure;
import xdev.db.StoredProcedure.Param;
import xdev.db.StoredProcedure.ParamType;
import xdev.db.StoredProcedure.ReturnTypeFlavor;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCMetaData;
import xdev.db.sql.Functions;
import xdev.db.sql.SELECT;
import xdev.db.sql.Table;
import xdev.util.ProgressMonitor;


public class PostgreSQLJDBCMetaData extends JDBCMetaData
{
	private static final long	serialVersionUID	= -4935821256152833016L;
	
	
	public PostgreSQLJDBCMetaData(PostgreSQLJDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	
	@Override
	public TableInfo[] getTableInfos(ProgressMonitor monitor, EnumSet<TableType> types)
			throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		
		List<TableInfo> list = new ArrayList<>();
		
		JDBCConnection connection = (JDBCConnection)this.dataSource.openConnection();
		
		try
		{
			if(types.contains(TableType.TABLE))
			{
				Result rs = connection
						.query("SELECT schemaname, tablename FROM pg_tables WHERE tablename !~* 'pg_*|sql_*'"
								+ " AND schemaname != 'information_schema'");
				while(rs.next() && !monitor.isCanceled())
				{
					list.add(new TableInfo(TableType.TABLE,rs.getString("schemaname"),rs
							.getString("tablename")));
				}
				rs.close();
			}
			
			if(types.contains(TableType.VIEW))
			{
				Result rs = connection
						.query("SELECT schemaname, viewname FROM pg_views WHERE viewname !~* 'pg_*|sql_*'"
								+ " AND schemaname != 'information_schema'");
				while(rs.next())
				{
					list.add(new TableInfo(TableType.VIEW,rs.getString("schemaname"),rs
							.getString("viewname")));
				}
				rs.close();
			}
		}
		finally
		{
			connection.close();
		}
		
		monitor.done();
		
		TableInfo[] tables = list.toArray(new TableInfo[list.size()]);
		Arrays.sort(tables);
		return tables;
	}
	
	
	@Override
	protected TableMetaData getTableMetaData(JDBCConnection jdbcConnection, DatabaseMetaData meta,
			int flags, TableInfo table) throws DBException, SQLException
	{
		String catalog = getCatalog(this.dataSource);
		String schema = getSchema(this.dataSource);
		
		String tableName = table.getName();
		Table tableIdentity = new Table(tableName,"META_DUMMY");
		
		Map<String, Object> defaultValues = new HashMap<>();
		ResultSet rs = meta.getColumns(catalog,schema,tableName,null);
		while(rs.next())
		{
			String columnName = rs.getString("COLUMN_NAME");
			Object defaultValue = rs.getObject("COLUMN_DEF");
			defaultValues.put(columnName,defaultValue);
		}
		rs.close();
		
		Map<String, ColumnMetaData> columnMap = new HashMap<>();
		
		SELECT select = new SELECT().FROM(tableIdentity).WHERE("1 = 0");
		Result result = jdbcConnection.query(select);
		int cc = result.getColumnCount();
		ColumnMetaData[] columns = new ColumnMetaData[cc];
		for(int i = 0; i < cc; i++)
		{
			ColumnMetaData column = result.getMetadata(i);
			
			Object defaultValue = column.getDefaultValue();
			if(defaultValue == null && defaultValues.containsKey(column.getName()))
			{
				defaultValue = defaultValues.get(column.getName());
			}
			defaultValue = checkDefaultValue(defaultValue,column);
			
			columns[i] = new ColumnMetaData(tableName,column.getName(),column.getCaption(),
					column.getType(),column.getLength(),column.getScale(),defaultValue,
					column.isNullable(),column.isAutoIncrement());
			columnMap.put(columns[i].getName(),columns[i]);
		}
		result.close();
		
		StringBuilder sb = new StringBuilder();
		for(String columnName : defaultValues.keySet())
		{
			Object defaultValue = defaultValues.get(columnName);
			ColumnMetaData column = columnMap.get(columnName);
			if(column.isAutoIncrement())
			{
				continue;
			}
			
			// postres stores default values like NULL::character varying
			String defaultValueString = String.valueOf(defaultValue);
			int dd = defaultValueString.indexOf("::");
			if(dd == -1)
			{
				continue;
			}
			
			if(sb.length() > 0)
			{
				sb.append(", ");
			}
			sb.append(defaultValueString.substring(0,dd));
			sb.append(" AS \"");
			sb.append(columnName);
			sb.append("\"");
		}
		
		if(sb.length() > 0)
		{
			try
			{
				String defaultValueQuery = "SELECT " + sb.toString();
				result = jdbcConnection.query(defaultValueQuery);
				if(result.next())
				{
					cc = result.getColumnCount();
					for(int i = 0; i < cc; i++)
					{
						String columnName = result.getMetadata(i).getName();
						ColumnMetaData column = columnMap.get(columnName);
						if(column != null)
						{
							if(column.isAutoIncrement())
							{
								column.setDefaultValue(null);
							}
							else
							{
								Object defaultValue = result.getObject(i);
								defaultValue = checkDefaultValue(defaultValue,column);
								column.setDefaultValue(defaultValue);
							}
						}
					}
				}
				result.close();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		
		Map<IndexInfo, Set<String>> indexMap = new Hashtable<>();
		int count = UNKNOWN_ROW_COUNT;
		
		if(table.getType() == TableType.TABLE)
		{
			if((flags & INDICES) != 0)
			{
				Set<String> primaryKeyColumns = new HashSet<>();
				rs = meta.getPrimaryKeys(catalog,schema,tableName);
				while(rs.next())
				{
					primaryKeyColumns.add(rs.getString("COLUMN_NAME"));
				}
				rs.close();
				if(primaryKeyColumns.size() > 0)
				{
					indexMap.put(new IndexInfo("PRIMARY_KEY",IndexType.PRIMARY_KEY),
							primaryKeyColumns);
				}
				
				rs = meta.getIndexInfo(catalog,schema,tableName,false,true);
				while(rs.next())
				{
					String indexName = rs.getString("INDEX_NAME");
					String columnName = rs.getString("COLUMN_NAME");
					if(columnName != null && columnName.length() > 1 && columnName.charAt(0) == '"'
							&& columnName.charAt(columnName.length() - 1) == '"')
					{
						// peculiarity of Postgre
						columnName = columnName.substring(1,columnName.length() - 1);
					}
					if(indexName != null && columnName != null
							&& !primaryKeyColumns.contains(columnName))
					{
						boolean unique = !rs.getBoolean("NON_UNIQUE");
						IndexInfo info = new IndexInfo(indexName,unique ? IndexType.UNIQUE
								: IndexType.NORMAL);
						Set<String> columnNames = indexMap.get(info);
						if(columnNames == null)
						{
							columnNames = new HashSet<>();
							indexMap.put(info,columnNames);
						}
						columnNames.add(columnName);
					}
				}
				rs.close();
			}
			
			if((flags & ROW_COUNT) != 0)
			{
				try
				{
					result = jdbcConnection.query(new SELECT().columns(Functions.COUNT()).FROM(
							tableIdentity));
					if(result.next())
					{
						count = result.getInt(0);
					}
					result.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		
		Index[] indices = new Index[indexMap.size()];
		int i = 0;
		for(IndexInfo indexInfo : indexMap.keySet())
		{
			Set<String> columnList = indexMap.get(indexInfo);
			String[] indexColumns = columnList.toArray(new String[columnList.size()]);
			indices[i++] = new Index(indexInfo.name,indexInfo.type,indexColumns);
		}
		
		return new TableMetaData(table,columns,indices,count);
	}
	
	
	@Override
	public StoredProcedure[] getStoredProcedures(ProgressMonitor monitor) throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		
		List<StoredProcedure> list = new ArrayList<>();
		
		try
		{
			ConnectionProvider connectionProvider = this.dataSource.getConnectionProvider();
			Connection connection = connectionProvider.getConnection();
			
			try
			{
				DatabaseMetaData meta = connection.getMetaData();
				String catalog = getCatalog(this.dataSource);
				String schema = getSchema(this.dataSource);
				
				ResultSet rs = meta.getProcedures(catalog,schema,null);
				while(rs.next() && !monitor.isCanceled())
				{
					String spschema = rs.getString("PROCEDURE_SCHEM");
					if(spschema != null
							&& (spschema.equalsIgnoreCase("information_schema") || spschema
									.equalsIgnoreCase("pg_catalog")))
					{
						continue;
					}
					
					String name = rs.getString("PROCEDURE_NAME");
					String description = rs.getString("REMARKS");
					
					ReturnTypeFlavor returnTypeFlavor;
					DataType returnType = null;
					int procedureType = rs.getInt("PROCEDURE_TYPE");
					switch(procedureType)
					{
						case DatabaseMetaData.procedureNoResult:
							returnTypeFlavor = ReturnTypeFlavor.VOID;
						break;
						
						default:
							returnTypeFlavor = ReturnTypeFlavor.UNKNOWN;
					}
					
					List<Param> params = new ArrayList<>();
					ResultSet rsp = meta.getProcedureColumns(catalog,schema,name,null);
					while(rsp.next())
					{
						int dataTypeValue = rsp.getInt("DATA_TYPE");
						DataType dataType = DataType.get(dataTypeValue);
						
						if(dataTypeValue == 1111)
						{
							// 1111 == Types.OTHER and handled as ResultSet
							returnTypeFlavor = ReturnTypeFlavor.RESULT_SET;
						}
						else
						{
							
							String columnName = rsp.getString("COLUMN_NAME");
							switch(rsp.getInt("COLUMN_TYPE"))
							{
								case DatabaseMetaData.procedureColumnReturn:
									returnTypeFlavor = ReturnTypeFlavor.TYPE;
									returnType = dataType;
								break;
								
								case DatabaseMetaData.procedureColumnResult:
									returnTypeFlavor = ReturnTypeFlavor.RESULT_SET;
								break;
								
								case DatabaseMetaData.procedureColumnIn:
									params.add(new Param(ParamType.IN,columnName,dataType));
								break;
								
								case DatabaseMetaData.procedureColumnOut:
									params.add(new Param(ParamType.OUT,columnName,dataType));
								break;
								
								case DatabaseMetaData.procedureColumnInOut:
									params.add(new Param(ParamType.IN_OUT,columnName,dataType));
								break;
							}
						}
					}
					rsp.close();
					
					list.add(new StoredProcedure(returnTypeFlavor,returnType,name,description,
							params.toArray(new Param[params.size()])));
				}
			}
			finally
			{
				connection.close();
			}
		}
		catch(SQLException e)
		{
			throw new DBException(this.dataSource,e);
		}
		
		monitor.done();
		
		return list.toArray(new StoredProcedure[list.size()]);
	}
	
	
	@Override
	protected void createTable(JDBCConnection jdbcConnection, TableMetaData table)
			throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" (");
		
		ColumnMetaData[] columns = table.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			
			ColumnMetaData column = columns[i];
			appendEscapedName(column.getName(),sb);
			sb.append(" ");
			appendColumnDefinition(column,sb,params);
		}
		
		for(Index index : table.getIndices())
		{
			sb.append(", ");
			appendIndexDefinition(index,sb);
		}
		
		sb.append(")");
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	
	
	@Override
	protected void addColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData columnBefore, ColumnMetaData columnAfter)
			throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD COLUMN ");
		appendEscapedName(column.getName(),sb);
		sb.append(" ");
		appendColumnDefinition(column,sb,params);
		if(columnBefore == null)
		{
			sb.append(" FIRST");
		}
		else
		{
			sb.append(" AFTER ");
			appendEscapedName(columnBefore.getName(),sb);
		}
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	
	
	@Override
	protected void alterColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData existing) throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" MODIFY COLUMN ");
		appendEscapedName(existing.getName(),sb);
		sb.append(" ");
		appendColumnDefinition(column,sb,params);
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	

	@SuppressWarnings("incomplete-switch")
	@Override
	public boolean equalsType(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		DataType clientType = clientColumn.getType();
		DataType dbType = dbColumn.getType();
		
		if(clientType == dbType)
		{
			switch(clientType)
			{
				case TINYINT:
				case SMALLINT:
				case INTEGER:
				case BIGINT:
				case REAL:
				case FLOAT:
				case DOUBLE:
				case DATE:
				case TIME:
				case TIMESTAMP:
				case BOOLEAN:
				{
					return true;
				}
				
				case NUMERIC:
				case DECIMAL:
				{
					return clientColumn.getLength() == dbColumn.getLength()
							&& clientColumn.getScale() == dbColumn.getScale();
				}
				
				case CHAR:
				case VARCHAR:
				case BINARY:
				case VARBINARY:
				{
					return clientColumn.getLength() == dbColumn.getLength();
				}
				
				case CLOB:
				case LONGVARCHAR:
				{
					return equalsTextLengthRange(clientColumn,dbColumn);
				}
				
				case BLOB:
				case LONGVARBINARY:
				{
					return equalsBinaryLengthRange(clientColumn,dbColumn);
				}
			}
		}
		
		Boolean match = getTypeMatch(clientColumn,dbColumn);
		if(match != null)
		{
			return match;
		}
		
		match = getTypeMatch(dbColumn,clientColumn);
		if(match != null)
		{
			return match;
		}
		
		return false;
	}
	

	@SuppressWarnings("incomplete-switch")
	private Boolean getTypeMatch(ColumnMetaData thisColumn, ColumnMetaData thatColumn)
	{
		DataType thisType = thisColumn.getType();
		DataType thatType = thatColumn.getType();
		
		switch(thisType)
		{
			case CLOB:
			case LONGVARCHAR:
			{
				return (thatType == DataType.CLOB || thatType == DataType.LONGVARBINARY)
						&& equalsTextLengthRange(thisColumn,thatColumn);
			}
			
			case BLOB:
			case LONGVARBINARY:
			{
				return (thatType == DataType.BLOB || thatType == DataType.LONGVARBINARY)
						&& equalsBinaryLengthRange(thisColumn,thatColumn);
			}
		}
		
		return null;
	}
	
	
	private boolean equalsTextLengthRange(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		int clientLength = clientColumn.getLength();
		int dbLength = dbColumn.getLength();
		
		if(clientLength == dbLength)
		{
			return true;
		}
		else if(clientLength <= 255)
		{
			return dbLength <= 255;
		}
		else if(clientLength <= 65535)
		{
			return dbLength <= 65535;
		}
		else if(clientLength <= 16777215)
		{
			return dbLength <= 16777215;
		}
		else if(clientLength > 16777215)
		{
			return dbLength > 16777215;
		}
		
		return false;
	}
	
	
	private boolean equalsBinaryLengthRange(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		int clientLength = clientColumn.getLength();
		int dbLength = dbColumn.getLength();
		
		if(clientLength == dbLength)
		{
			return true;
		}
		else if(clientLength <= 255)
		{
			return dbLength <= 255;
		}
		else if(clientLength <= 65535)
		{
			return dbLength <= 65535;
		}
		else if(clientLength <= 16777215)
		{
			return dbLength <= 16777215;
		}
		else if(clientLength <= 4294967295l)
		{
			return dbLength <= 4294967295l;
		}
		else if(clientLength > 4294967295l)
		{
			return dbLength > 4294967295l;
		}
		
		return false;
	}
	

	@SuppressWarnings("incomplete-switch")
	private void appendColumnDefinition(ColumnMetaData column, StringBuilder sb, List params)
	{
		DataType type = column.getType();
		switch(type)
		{
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case REAL:
			case FLOAT:
			case DOUBLE:
			case DATE:
			case TIME:
			case TIMESTAMP:
			{
				sb.append(type.name());
			}
			break;
			
			case NUMERIC:
			case DECIMAL:
			{
				sb.append(type.name());
				sb.append("(");
				sb.append(column.getLength());
				sb.append(",");
				sb.append(column.getScale());
				sb.append(")");
			}
			break;
			
			case BOOLEAN:
			{
				sb.append("BOOL");
			}
			break;
			
			case CHAR:
			case VARCHAR:
			case BINARY:
			case VARBINARY:
			{
				sb.append(type.name());
				sb.append("(");
				sb.append(column.getLength());
				sb.append(")");
			}
			break;
			
			case CLOB:
			case LONGVARCHAR:
			{
				int length = column.getLength();
				if(length <= 255)
				{
					sb.append("TINYTEXT");
				}
				else if(length <= 65535)
				{
					sb.append("TEXT");
				}
				else if(length <= 16777215)
				{
					sb.append("MEDIUMTEXT");
				}
				else
				{
					sb.append("LONGTEXT");
				}
			}
			break;
			
			case BLOB:
			case LONGVARBINARY:
			{
				int length = column.getLength();
				if(length <= 255)
				{
					sb.append("TINYBLOB");
				}
				else if(length <= 65535)
				{
					sb.append("BLOB");
				}
				else if(length <= 16777215)
				{
					sb.append("MEDIUMBLOB");
				}
				else if(length <= 4294967295l)
				{
					sb.append("LONGBLOB");
				}
				else
				{
					sb.append("BLOB");
				}
			}
			break;
		}
		
		if(column.isNullable())
		{
			sb.append(" NULL");
		}
		else
		{
			sb.append(" NOT NULL");
		}
		
		Object defaultValue = column.getDefaultValue();
		if(!(defaultValue == null && !column.isNullable()))
		{
			sb.append(" DEFAULT ");
			if(defaultValue == null)
			{
				sb.append("NULL");
			}
			else
			{
				sb.append("?");
				params.add(defaultValue);
			}
		}
		
		if(column.isAutoIncrement())
		{
			sb.append(" AUTO_INCREMENT");
		}
	}
	
	
	@Override
	protected void dropColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column) throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" DROP COLUMN ");
		appendEscapedName(column.getName(),sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	@Override
	protected void createIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD ");
		appendIndexDefinition(index,sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	private void appendIndexDefinition(Index index, StringBuilder sb)
	{
		switch(index.getType())
		{
			case PRIMARY_KEY:
			{
				sb.append("PRIMARY KEY");
			}
			break;
			
			case UNIQUE:
			{
				sb.append("UNIQUE INDEX ");
				appendEscapedName(index.getName(),sb);
			}
			break;
			
			case NORMAL:
			{
				sb.append("INDEX ");
				appendEscapedName(index.getName(),sb);
			}
			break;
		}
		
		sb.append(" (");
		String[] columns = index.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			appendEscapedName(columns[i],sb);
		}
		sb.append(")");
	}
	
	
	@Override
	protected void dropIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		
		if(index.getType() == IndexType.PRIMARY_KEY)
		{
			sb.append(" DROP PRIMARY KEY");
		}
		else
		{
			sb.append(" DROP INDEX ");
			appendEscapedName(index.getName(),sb);
		}
		
		jdbcConnection.write(sb.toString());
	}
	
	
	@Override
	protected void appendEscapedName(String name, StringBuilder sb)
	{
		sb.append("`");
		sb.append(name);
		sb.append("`");
	}
}
