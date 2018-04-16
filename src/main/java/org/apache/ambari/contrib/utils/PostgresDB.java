/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ambari.contrib.utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresDB {

    private Connection conn = null;
    private Statement stmt = null;
    private Map<String, Long> rowCounts = new HashMap<>();

    private PostgresDB() {
    }

    protected static final String[] TABLE_TYPES = {"TABLE"};

    public PostgresDB(String url, String user, String password) {
        try {
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to create database connection " + url);
        }
    }

    List<String> getTables() {
        List<String> ret = new ArrayList<>();
        try {
            DatabaseMetaData md = conn.getMetaData();
            String[] types = new String[]{"TABLE"};
            ResultSet rs = md.getTables(null, null, "%", TABLE_TYPES);
            while (rs.next()) {
                Object column = rs.getObject(3);
                ret.add(column.toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ret;
    }


    ResultSet getRows(String tablename) {
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery("SELECT * FROM " + tablename + ";");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }
}
