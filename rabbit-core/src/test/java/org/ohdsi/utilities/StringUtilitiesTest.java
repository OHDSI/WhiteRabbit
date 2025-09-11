package org.ohdsi.utilities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StringUtilitiesTest {

    @Test
    void isDate() {
        // previously supported date formats: yyyy.mm.dd or mm.dd.yy ("." may be any separator)
        assertTrue(StringUtilities.isDate("31.01.23"));
        assertTrue(StringUtilities.isDate("31.12.32"));
        assertTrue(StringUtilities.isDate("12.31.32"));
        assertTrue(StringUtilities.isDate("2023.01.31"));
        assertTrue(StringUtilities.isDate("2032.12.31"));
        assertFalse(StringUtilities.isDate("2032.12.32"));

        // more generally supported date formats after implementing issue #313 (copilot generated)
        assertTrue(StringUtilities.isDate("2023-01-01"));
        assertTrue(StringUtilities.isDate("01/01/2023"));
        assertTrue(StringUtilities.isDate("01-01-2023"));
        assertTrue(StringUtilities.isDate("01.01.2023"));
        assertTrue(StringUtilities.isDate("2023/01/01"));
        assertTrue(StringUtilities.isDate("2023.01.01"));
        assertTrue(StringUtilities.isDate("2023 01 01"));
        assertTrue(StringUtilities.isDate("01 01 2023"));
        assertFalse(StringUtilities.isDate("2023-13-01"));
        assertFalse(StringUtilities.isDate("2023-01-32"));
        assertFalse(StringUtilities.isDate("2023/01/32"));
        assertFalse(StringUtilities.isDate("not a date"));

        assertTrue(StringUtilities.isDate("2023-12-31"));
        assertTrue(StringUtilities.isDate("12-31-2023"));
        assertTrue(StringUtilities.isDate("12-13-2023"));
        assertTrue(StringUtilities.isDate("13-12-2023"));
        assertFalse(StringUtilities.isDate("13-13-2023"));

    }
}