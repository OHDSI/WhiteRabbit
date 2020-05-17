package org.ohdsi.utilities;

import java.io.IOException;
import java.util.Properties;

public class Version {

    public static String getVersion(Class c) {
        final Properties properties = new Properties();
        try {
            properties.load(c.getClassLoader().getResourceAsStream("project.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties.getProperty("version");
    }
}
