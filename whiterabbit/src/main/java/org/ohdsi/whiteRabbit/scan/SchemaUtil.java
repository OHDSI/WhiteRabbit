package org.ohdsi.whiteRabbit.scan;

import org.ohdsi.databases.DbType;
import org.ohdsi.whiteRabbit.DbSettings;

import java.util.function.IntPredicate;

public class SchemaUtil {
    public static String adaptSchemaNameForPostgres(DbSettings dbSettings, String schema) {
        if (dbSettings.dbType == DbType.POSTGRESQL && containsUpperCase(schema)) {
            return String.format("\"%s\"", schema);
        } else {
            return schema;
        }
    }

    private static boolean containsUpperCase(String value) {
        return contains(value, i -> Character.isLetter(i) && Character.isUpperCase(i));
    }

    private static boolean contains(String value, IntPredicate predicate) {
        return value.chars().anyMatch(predicate);
    }
}
