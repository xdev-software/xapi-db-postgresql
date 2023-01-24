package xdev.db.postgresql.jdbc;

import static com.xdev.jadoth.sqlengine.SQL.LANG.DEFAULT_VALUES;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation._;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.dot;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.ASEXPRESSION;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.OMITALIAS;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.UNQUALIFIED;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.indent;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.isOmitAlias;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.isSingleLine;
import static com.xdev.jadoth.sqlengine.internal.interfaces.TableExpression.Utils.getAlias;

import com.xdev.jadoth.sqlengine.INSERT;
import com.xdev.jadoth.sqlengine.SELECT;
import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler;
import com.xdev.jadoth.sqlengine.internal.AssignmentValuesClause;
import com.xdev.jadoth.sqlengine.internal.QueryPart;
import com.xdev.jadoth.sqlengine.internal.SqlColumn;
import com.xdev.jadoth.sqlengine.internal.interfaces.TableExpression;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;


public class PostgreSQLDMLAssembler extends StandardDMLAssembler<PostgreSQLDbms>
{
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	public PostgreSQLDMLAssembler(final PostgreSQLDbms dbms)
	{
		super(dbms);
	}
	
	
	@Override
	public StringBuilder assembleColumn(final SqlColumn column, final StringBuilder sb,
			final int indentLevel, int flags)
	{
		final TableExpression owner = column.getOwner();
		
		final DbmsAdaptor<?> dbms = this.getDbmsAdaptor();
		final String columnName = column.getColumnName();
		final boolean delimColumn = (dbms.getConfiguration().isDelimitColumnIdentifiers() || QueryPart
				.isDelimitColumnIdentifiers(flags))
				&& (columnName != null && !"*".equals(columnName));
		final char delimiter = dbms.getIdentifierDelimiter();
		
		flags |= QueryPart.bitDelimitColumnIdentifiers(this.getDbmsAdaptor().getConfiguration()
				.isDelimitColumnIdentifiers());
		
		if(owner != null && !QueryPart.isUnqualified(flags))
		{
			this.assembleColumnQualifier(column,sb,flags);
		}
		if(delimColumn)
		{
			sb.append(delimiter);
		}
		QueryPart.assembleObject(column.getExpressionObject(),this,sb,indentLevel,flags);
		if(delimColumn)
		{
			sb.append(delimiter);
		}
		return sb;
	}
	
	
	@Override
	public StringBuilder assembleColumnQualifier(final SqlColumn column, final StringBuilder sb,
			final int flags)
	{
		final TableExpression owner = column.getOwner();
		String qualifier = getAlias(owner);
		if(qualifier == null || QueryPart.isQualifyByTable(flags))
		{
			if(owner instanceof SqlTableIdentity)
			{
				return assembleTableIdentifier((SqlTableIdentity)owner,sb,0,flags).append(dot);
			}
			else
			{
				qualifier = owner.toString();
			}
		}
		final char delimiter = getDbmsAdaptor().getIdentifierDelimiter();
		return sb.append(delimiter).append(qualifier).append(delimiter).append(dot);
	}
	
	
	@Override
	public StringBuilder assembleTableIdentifier(SqlTableIdentity table, StringBuilder sb,
			int indentLevel, int flags)
	{
		final DbmsAdaptor<?> dbms = this.getDbmsAdaptor();
		
		final SqlTableIdentity.Sql sql = table.sql();
		final String schema = sql.schema;
		final String name = sql.name;
		final char delimiter = dbms.getIdentifierDelimiter();
		
		if(schema != null)
		{
			sb.append(schema).append(dot);
		}
		sb.append(delimiter);
		sb.append(name);
		sb.append(delimiter);
		
		if(!isOmitAlias(flags))
		{
			final String alias = sql.alias;
			if(alias != null && alias.length() > 0)
			{
				sb.append(_);
				sb.append(delimiter).append(alias).append(delimiter);
			}
		}
		return sb;
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	/**
	 * @param query
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleSELECT(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, int, java.lang.String,
	 *      java.lang.String)
	 */
	@Override
	protected StringBuilder assembleSELECT(final SELECT query, final StringBuilder sb,
			final int indentLevel, final int flags, final String clauseSeperator,
			final String newLine)
	{
		indent(sb,indentLevel,isSingleLine(flags)).append(query.keyword());
		this.assembleSelectDISTINCT(query,sb,indentLevel,flags);
		this.assembleSelectItems(query,sb,flags,indentLevel,newLine);
		this.assembleSelectSqlClauses(query,sb,indentLevel,flags | ASEXPRESSION,clauseSeperator,
				newLine);
		this.assembleAppendSELECTs(query,sb,indentLevel,flags,clauseSeperator,newLine);
		this.assembleSelectRowLimit(query,sb,flags,clauseSeperator,newLine,indentLevel);
		return sb;
	}
	
	
	/**
	 * @param query
	 * @param sb
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @param indentLevel
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler#assembleSelectRowLimit(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, java.lang.String, java.lang.String,
	 *      int)
	 */
	@Override
	protected StringBuilder assembleSelectRowLimit(final SELECT query, final StringBuilder sb,
			final int flags, final String clauseSeperator, final String newLine,
			final int indentLevel)
	{
		final Integer offset = query.getOffsetSkipCount();
		final Integer limit = query.getFetchFirstRowCount();
		
		if(offset != null && limit != null)
		{
			sb.append(newLine).append(clauseSeperator).append("LIMIT ").append(limit)
					.append(" OFFSET ").append(offset);
		}
		else if(limit != null)
		{
			sb.append(newLine).append(clauseSeperator).append("LIMIT ").append(limit);
		}
		return sb;
	}
	
	
	@Override
	protected StringBuilder assembleINSERT(INSERT query, StringBuilder sb, int flags,
			String clauseSeperator, String newLine, int indentLevel)
	{
		indent(sb,indentLevel,isSingleLine(flags)).append(query.keyword()).append(_INTO_);
		
		this.assembleTableIdentifier(query.getTable(),sb,indentLevel,flags | OMITALIAS);
		sb.append(newLine);
		
		this.assembleAssignmentColumnsClause(query,query.getColumnsClause(),sb,indentLevel,flags
				| UNQUALIFIED);
		sb.append(newLine);
		
		final SELECT valueSelect = query.filterSelect();
		if(valueSelect != null)
		{
			sb.append(clauseSeperator);
			QueryPart.assembleObject(valueSelect,this,sb,indentLevel,flags);
		}
		else
		{
			final AssignmentValuesClause values = query.getValuesClause();
			if(values != null)
			{
				this.assembleAssignmentValuesClause(query,values,sb,indentLevel,flags);
			}
			else
			{
				indent(sb,indentLevel,isSingleLine(flags)).append(DEFAULT_VALUES);
			}
		}
		
		return sb;
	}
}
