package com.arcadia.whiteRabbitService.util;

import com.arcadia.whiteRabbitService.model.scandata.ScanDbSettings;
import com.arcadia.whiteRabbitService.model.scandata.ScanFilesSettings;
import com.arcadia.whiteRabbitService.service.error.DbTypeNotSupportedException;
import com.arcadia.whiteRabbitService.service.error.DelimitedTextFileNotSupportedException;
import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.whiteRabbit.DbSettings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class DbSettingsAdapter {

    /**
     * Tested only Postgres and MS Sql
     * */
    private static final Map<DbType, Function<String, Boolean>> dbTypeIdentifiers = Map.of(
            DbType.MYSQL, dbType -> dbType.equalsIgnoreCase("MySQL"),
            DbType.ORACLE, dbType -> dbType.equalsIgnoreCase("Oracle"),
            DbType.POSTGRESQL, dbType -> dbType.equalsIgnoreCase("PostgreSQL"),
            DbType.REDSHIFT, dbType -> dbType.equalsIgnoreCase("Redshift"),
            DbType.MSSQL, dbType -> dbType.equalsIgnoreCase("SQL Server"),
            DbType.AZURE, dbType -> dbType.equalsIgnoreCase("Azure"),
            DbType.MSACCESS, dbType -> dbType.equalsIgnoreCase("MS Access"),
            DbType.TERADATA, dbType -> dbType.equalsIgnoreCase("Teradata"),
            DbType.BIGQUERY, dbType -> dbType.equalsIgnoreCase("BigQuery")
    );

    private static final List<DbType> dbTypesHasWindowsAuthentication = List.of(
            DbType.MSSQL,
            DbType.AZURE,
            DbType.PDW
    );

    private static final List<DbType> dbTypesHasDomain = List.of(
            DbType.BIGQUERY
    );

    private static final List<DbType> dbRequireSchema = List.of(
            DbType.POSTGRESQL,
            DbType.ORACLE
    );

    /**
     * Not supported SAS files
     * After implement supporting SAS files add: DbSettings.SourceType.SAS_FILES, fileType -> fileType.equals("sas7bdat")
     **/
    private static final Map<DbSettings.SourceType, Function<String, Boolean>> sourceTypeForDelimitedTextFileIdentifiers = Map.of(
            DbSettings.SourceType.CSV_FILES, fileType -> fileType.toLowerCase().contains("csv")
    );

    public static DbSettings adaptDbSettings(ScanDbSettings dbSetting) throws DbTypeNotSupportedException {
        DbSettings dbSettings = new DbSettings();

        dbSettings.sourceType = DbSettings.SourceType.DATABASE;
        dbSettings.user = dbSetting.getUser();
        dbSettings.password = dbSetting.getPassword();
        dbSettings.server = adaptServer(dbSetting.getServer(), dbSetting.getPort());
        dbSettings.database = dbSetting.getDatabase();
        dbSettings.dbType = adaptDbType(dbSetting.getDbType());

        checkWindowsAuthentication(dbSettings);
        setDomain(dbSettings);
        adaptSchemaName(dbSettings, dbSetting.getSchema());
        setTablesToScan(dbSettings, dbSetting.getTablesToScan());

        return dbSettings;
    }

    public static DbSettings adaptDelimitedTextFileSettings(ScanFilesSettings dto) throws DelimitedTextFileNotSupportedException {
        DbSettings dbSettings = new DbSettings();

        dbSettings.sourceType = adaptDelimitedFileTypeToSourceType(dto.getFileType());
        dbSettings.delimiter = dto.getDelimiter().charAt(0);
        dbSettings.tables = dto.getFileNames();
        return dbSettings;
    }

    public static DbType adaptDbType(String type) throws DbTypeNotSupportedException {
        for (Map.Entry<DbType, Function<String, Boolean>> entry : dbTypeIdentifiers.entrySet()) {
            if (entry.getValue().apply(type)) {
                return entry.getKey();
            }
        }

        throw new DbTypeNotSupportedException();
    }

    private static void checkWindowsAuthentication(DbSettings dbSettings) {
        if (dbTypesHasWindowsAuthentication.contains(dbSettings.dbType)) {
            if (dbSettings.user.length() != 0) { // Not using windows authentication
                String[] parts = dbSettings.user.split("/");
                if (parts.length == 2) {
                    dbSettings.user = parts[1];
                    dbSettings.domain = parts[0];
                }
            }
        }
    }

    private static void setDomain(DbSettings dbSettings) {
        if (dbTypesHasDomain.contains(dbSettings.dbType)) {
            dbSettings.domain = dbSettings.database;
        }
    }

    private static void setTablesToScan(DbSettings dbSettings, String tablesToScan) {
        if (tablesToScan != null) {
            if (tablesToScan.equals("*")) {
                try (RichConnection connection = new RichConnection(
                        dbSettings.server, dbSettings.domain,
                        dbSettings.user, dbSettings.password,
                        dbSettings.dbType
                )) {
                    dbSettings.tables.addAll(connection.getTableNames(dbSettings.database));
                }
            } else {
                dbSettings.tables.addAll(Arrays.asList(tablesToScan.split(",")));
            }
        }
    }

    private static String adaptServer(String server, int port) {
        return String.format("%s:%d", server, port);
    }

    private static void adaptSchemaName(DbSettings dbSettings, String schemaName) {
        if (dbRequireSchema.contains(dbSettings.dbType)) {
            dbSettings.server = String.format("%s/%s", dbSettings.server, dbSettings.database);
            dbSettings.database = schemaName;
        }
    }

    private static DbSettings.SourceType adaptDelimitedFileTypeToSourceType(String fileType) throws DelimitedTextFileNotSupportedException {
        for (Map.Entry<DbSettings.SourceType, Function<String, Boolean>> entry : sourceTypeForDelimitedTextFileIdentifiers.entrySet()) {
            if (entry.getValue().apply(fileType)) {
                return entry.getKey();
            }
        }

        throw new DelimitedTextFileNotSupportedException();
    }
}
