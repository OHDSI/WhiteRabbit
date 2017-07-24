package org.ohdsi.rabbitInAHat.dataModel.Db;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.ohdsi.rabbitInAHat.dataModel.Database;
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.Table;

public class RedshiftDatabase implements DbOperations {

	public static String[] keywords = new String[] { "AES128", "AES256", "ALL", "ALLOWOVERWRITE", "ANALYSE", "ANALYZE",
			"AND", "ANY", "ARRAY", "AS", "ASC", "AUTHORIZATION", "BACKUP", "BETWEEN", "BINARY", "BLANKSASNULL", "BOTH",
			"BYTEDICT", "BZIP2", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "CONSTRAINT", "CREATE", "CREDENTIALS",
			"CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURRENT_USER_ID", "DEFAULT",
			"DEFERRABLE", "DEFLATE", "DEFRAG", "DELTA", "DELTA32K", "DESC", "DISABLE", "DISTINCT", "DO", "ELSE",
			"EMPTYASNULL", "ENABLE", "ENCODE", "ENCRYPT", "ENCRYPTION", "END", "EXCEPT", "EXPLICIT", "FALSE", "FOR",
			"FOREIGN", "FREEZE", "FROM", "FULL", "GLOBALDICT256", "GLOBALDICT64K", "GRANT", "GROUP", "GZIP", "HAVING",
			"IDENTITY", "IGNORE", "ILIKE", "IN", "INITIALLY", "INNER", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN",
			"LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "LUN", "LUNS", "LZO", "LZOP", "MINUS",
			"MOSTLY13", "MOSTLY32", "MOSTLY8", "NATURAL", "NEW", "NOT", "NOTNULL", "NULL", "NULLS", "OFF", "OFFLINE",
			"OFFSET", "OID", "OLD", "ON", "ONLY", "OPEN", "OR", "ORDER", "OUTER", "OVERLAPS", "PARALLEL", "PARTITION",
			"PERCENT", "PERMISSIONS", "PLACING", "PRIMARY", "RAW", "READRATIO", "RECOVER", "REFERENCES", "RESPECT",
			"REJECTLOG", "RESORT", "RESTORE", "RIGHT", "SELECT", "SESSION_USER", "SIMILAR", "SNAPSHOT ", "SOME",
			"SYSDATE", "SYSTEM", "TABLE", "TAG", "TDES", "TEXT255", "TEXT32K", "THEN", "TIMESTAMP", "TO", "TOP",
			"TRAILING", "TRUE", "TRUNCATECOLUMNS", "UNION", "UNIQUE", "USER", "USING", "VERBOSE", "WALLET", "WHEN",
			"WHERE", "WITH", "WITHOUT" };

	private static Set<String> keywordSet;

	private Database database;

	public Database getDatabase() {
		return this.database;
	}

	public RedshiftDatabase(Database db) {
		this.database = db;

		keywordSet = new HashSet<String>();
		for (String keyword : keywords)
			keywordSet.add(keyword);
	}

	@Override
	public String dropTableIfExists(String table) {
		return String.format("DROP TABLE IF EXISTS %s;", convertToSqlName(table));
	}

	@Override
	public String createTestResults() {
		return "CREATE TABLE test_results "
				+ "AS SELECT TOP 0 * FROM (SELECT 0 as id, cast('' as varchar(512)) as description, "
				+ "cast('' as varchar(256)) as test, cast('' as varchar(5)) as status)";
	}

	@Override
	public String convertToSqlName(String name) {
		if (name.startsWith("\"") && name.endsWith("\""))
			return name;

		name = name.replace('\"', '_');

		if (name.contains(" ") || name.contains(".") || keywordSet.contains(name.toUpperCase()))
			return "\"" + name + "\"";

		return name;
	}

	@Override
	public String clearTable(String table) {
		return dropTableIfExists(table + "_ctas") + "\n" + String.format("CREATE TABLE %s AS SELECT TOP 0 * FROM %s",
				convertToSqlName(table + "_ctas"), convertToSqlName(table));
	}

	@Override
	public String renameTable(String oldName, String newName) {
		return String.format("ALTER TABLE %s RENAME to %s;", convertToSqlName(oldName), convertToSqlName(newName));
	}

	@Override
	public String getExpectTestLine() {
		return "UNION ALL SELECT ";
	}

	@Override
	public List<String> getInsertValues(Table table) {
		List<String> result = new ArrayList<String>();
		for (Field field : table.getFields()) {
			String rFieldName = field.getName().replaceAll(" ", "_").replaceAll("-", "_");

			result.add("  if (missing(" + rFieldName + ")) {");
			result.add("    " + rFieldName + " <- defaults$" + rFieldName);
			result.add("    if (is.null(" + rFieldName + ")) " + rFieldName + " <- NA");
			result.add("  }");
			result.add("  if (is.null(" + rFieldName + ")) " + rFieldName + " <- NA");
			result.add("  insertValues <- c(insertValues, " + rFieldName + ")");
			result.add("");
		}
		return result;
	}

	@Override
	public String getInsertStatement(Table table) {
		StringBuilder line = new StringBuilder();
		line.append("  isnull <- function(v) { if (is.na(v)) \"NULL\" else paste0(\"'\", v, \"'\") }\n");
		line.append("  insertValues <- lapply(insertValues, FUN = isnull)\n");
		line.append("  statement <- paste0(\"UNION ALL SELECT \", ");
		line.append("paste(insertValues, collapse = \", \"))");

		return line.toString();
	}

	@Override
	public String getTableFooter() {
		return "paste0('ALTER TABLE ', t, '_ctas', ' RENAME to ', t, ';')";
	}

	@Override
	public String dropTableIfExists() {
		return "paste0('DROP TABLE IF EXISTS ', t, ';')";
	}
}
