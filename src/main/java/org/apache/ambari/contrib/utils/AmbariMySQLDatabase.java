package org.apache.ambari.contrib.utils;

import java.sql.SQLException;
import java.sql.Statement;

public class AmbariMySQLDatabase extends AmbariDatabase {

    public AmbariMySQLDatabase(String url, String user, String password) {
        super(url, user, password);
    }

    public void enableConstraints() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=1");
    }

    public void disableConstraints() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=0");
    }
}
