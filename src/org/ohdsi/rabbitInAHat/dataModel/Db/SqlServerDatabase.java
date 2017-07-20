package org.ohdsi.rabbitInAHat.dataModel.Db;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.ohdsi.rabbitInAHat.dataModel.Database;
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.Table;

public class SqlServerDatabase implements DbOperations {
	
	public static String[]		keywords	= new String[] { "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "BACKUP", "BEGIN", "BETWEEN",
		"BREAK", "BROWSE", "BULK", "BY", "CASCADE", "CASE", "CHECK", "CHECKPOINT", "CLOSE", "CLUSTERED", "COALESCE", "COLLATE", "COLUMN", "COMMIT",
		"COMPUTE", "CONSTRAINT", "CONTAINS", "CONTAINSTABLE", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME",
		"CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DBCC", "DEALLOCATE", "DECLARE", "DEFAULT", "DELETE", "DENY", "DESC", "DISK",
		"DISTINCT", "DISTRIBUTED", "DOUBLE", "DROP", "DUMP", "ELSE", "END", "ERRLVL", "ESCAPE", "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXIT", "EXTERNAL",
		"FETCH", "FILE", "FILLFACTOR", "FOR", "FOREIGN", "FREETEXT", "FREETEXTTABLE", "FROM", "FULL", "FUNCTION", "GOTO", "GRANT", "GROUP", "HAVING",
		"HOLDLOCK", "IDENTITY", "IDENTITY_INSERT", "IDENTITYCOL", "IF", "IN", "INDEX", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN", "KEY", "KILL",
		"LEFT", "LIKE", "LINENO", "LOAD", "MERGE", "NATIONAL", "NOCHECK", "NONCLUSTERED", "NOT", "NULL", "NULLIF", "OF", "OFF", "OFFSETS", "ON", "OPEN",
		"OPENDATASOURCE", "OPENQUERY", "OPENROWSET", "OPENXML", "OPTION", "OR", "ORDER", "OUTER", "OVER", "PERCENT", "PIVOT", "PLAN", "PRECISION",
		"PRIMARY", "PRINT", "PROC", "PROCEDURE", "PUBLIC", "RAISERROR", "READ", "READTEXT", "RECONFIGURE", "REFERENCES", "REPLICATION", "RESTORE",
		"RESTRICT", "RETURN", "REVERT", "REVOKE", "RIGHT", "ROLLBACK", "ROWCOUNT", "ROWGUIDCOL", "RULE", "SAVE", "SCHEMA", "SECURITYAUDIT", "SELECT",
		"SEMANTICKEYPHRASETABLE", "SEMANTICSIMILARITYDETAILSTABLE", "SEMANTICSIMILARITYTABLE", "SESSION_USER", "SET", "SETUSER", "SHUTDOWN", "SOME",
		"STATISTICS", "SYSTEM_USER", "TABLE", "TABLESAMPLE", "TEXTSIZE", "THEN", "TO", "TOP", "TRAN", "TRANSACTION", "TRIGGER", "TRUNCATE", "TRY_CONVERT",
		"TSEQUAL", "UNION", "UNIQUE", "UNPIVOT", "UPDATE", "UPDATETEXT", "USE", "USER", "VALUES", "VARYING", "VIEW", "WAITFOR", "WHEN", "WHERE", "WHILE",
		"WITH", "WITHIN GROUP", "WRITETEXT" };
	
	private static Set<String> keywordSet;
	
	private Database database;	 
	
	public SqlServerDatabase(Database db) {
		this.database = db;
		
		keywordSet = new HashSet<String>();
		for (String keyword : keywords)
			keywordSet.add(keyword);
	}
	
	public Database getDatabase(){
		return this.database;
	}	
		
	@Override
	public String dropTableIfExists(String table) {
		return String.format(
				"IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s;",
				convertToSqlName(table),
				convertToSqlName(table)
				);
	}

	@Override
	public String createTestResults() {
		return "CREATE TABLE test_results (id INT, description VARCHAR(512), test VARCHAR(256), status VARCHAR(5));";
	}

	@Override
	public String convertToSqlName(String name) {
		if (name.startsWith("[") && name.endsWith("]"))
			return name;
		
		name = name.replace('[', '_').replace(']', '_');
		
		if (name.contains(" ") || name.contains(".") || keywordSet.contains(name.toUpperCase()))
			return "[" + name + "]";

		return name;
	}

	@Override
	public String clearTable(String table) {
		return String.format("TRUNCATE TABLE %s;", convertToSqlName(table));		
	}

	@Override
	public String renameTable(String oldName, String newName) {
		return String.format("EXEC sp_rename '%s', '%s';", 
				convertToSqlName(oldName), 
				convertToSqlName(newName)); 
		
	}

	@Override
	public List<String> getInsertValues(Table table) {
		List<String> result = new ArrayList<String>();
		for (Field field : table.getFields()) {
			String rFieldName = field.getName().replaceAll(" ", "_").replaceAll("-", "_");
			String sqlFieldName = this.convertToSqlName(field.getName());
			
			result.add("  if (missing(" + rFieldName + ")) {");
			result.add("    " + rFieldName + " <- defaults$" + rFieldName);
			result.add("  }");
			result.add("  if (!is.null(" + rFieldName + ")) {");
			result.add("    insertFields <- c(insertFields, \"" + sqlFieldName + "\")");
			result.add("    insertValues <- c(insertValues, " + rFieldName + ")");
			result.add("  }");
			result.add("");
		}
		return result;
	}

	@Override
	public String getInsertStatement(Table table) {
		StringBuilder line = new StringBuilder();
		line.append("  statement <- paste0(\"INSERT INTO " +
				this.convertToSqlName(table.getName()) + " (\", ");
		line.append("paste(insertFields, collapse = \", \"), ");
		line.append("\") VALUES ('\", ");
		line.append("paste(insertValues, collapse = \"', '\"), ");
		line.append("\"');\")");
		return line.toString();
	}

	@Override
	public String getExpectTestLine() {		
		return "INSERT INTO test_results SELECT ";
	}

	@Override
	public String dropTableIfExists() {
		return "paste0('IF OBJECT_ID(', t, ', \'U\') IS NOT NULL DROP TABLE ', t, ';')";
	}

	@Override
	public String getTableFooter() {
		return "";
	}
}
