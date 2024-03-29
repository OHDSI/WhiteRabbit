package org.ohdsi.databases;

import org.junit.jupiter.api.Test;
import org.ohdsi.databases.configuration.DbType;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

import static org.junit.jupiter.api.Assertions.*;

class DBConnectorTest {

    public static void main(String[] args) {
        DBConnectorTest dbConnectorTest = new DBConnectorTest();
        dbConnectorTest.verifyDrivers();
    }

    @Test
    void verifyDrivers() {
        // verify that a JDBC driver that is not included/supported cannot be loaded
        String notSupportedDriver = "org.sqlite.JDBC"; // change this if WhiteRabbit starts supporting SQLite
        assertFalse(DbType.driverNames().contains(notSupportedDriver), "Cannot test this for a supported driver.");
        assertThrows(ClassNotFoundException.class, () ->
            testJDBCDriverAndVersion(notSupportedDriver));
        DbType.includedDriverNames().forEach(driver -> {
            try {
                testJDBCDriverAndVersion(driver);
            } catch (ClassNotFoundException e) {
                fail(String.format("JDBC driver class could not be loaded for %s", driver));
            }
        });
        System.out.println("All configured JDBC drivers could be loaded.");
    }

    void testJDBCDriverAndVersion(String driverName) throws ClassNotFoundException {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            Class<?> driverClass = Class.forName(driverName);
            if (driver.getClass().isAssignableFrom(driverClass)) {
                int ignoredMajorVersion = driver.getMajorVersion();
            }
        }
    }

    @Test
    void createMySQLUrl() {
        assertTrue(DBConnector.createMySQLUrl("127.0.0.1").contains(":3306/"),
                "The default port (:3306) should have been added when no port is specified in the server string");
        assertTrue(DBConnector.createMySQLUrl("127.0.0.1/").contains(":3306"),
                "The default port (:3306) should have been added when no port is specified in the server string");
        assertFalse(DBConnector.createMySQLUrl("127.0.0.1:12345/").contains(":3306"),
                "The default port (:3306) should not have been added when a port is specified in the server string");
    }
}