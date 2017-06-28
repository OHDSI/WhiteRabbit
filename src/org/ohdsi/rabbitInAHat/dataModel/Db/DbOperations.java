package org.ohdsi.rabbitInAHat.dataModel.Db;

import org.ohdsi.rabbitInAHat.dataModel.Database;

/// SQL expressions for operations on specific database management system
public interface DbOperations {
	Database getDatabase();
	String clearTable(String table);
	String dropTableIfExists(String table);
	String createTestResults();
	String convertToSqlName(String name);
	
}
