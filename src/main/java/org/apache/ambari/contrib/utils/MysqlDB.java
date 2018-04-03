/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ambari.contrib.utils;

import java.sql.*;
import java.util.List;

public class MysqlDB {
    private Connection conn =  null;

    private MysqlDB() {
    }

    public MysqlDB(String url, String user, String password) {
        try {
            conn = DriverManager.getConnection(url, user, password);

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to create database connection " + url);
        }
    }

    public void deleteFromTable(String tablename) throws SQLException{
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("TRUNCATE TABLE " + normalizeTablename(tablename));
    }

    public PreparedStatement buildInsertStatement(String tablename, List<String> columnNames) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO " + normalizeTablename(tablename) + "(");
        for(int i = 0 ; i<columnNames.size(); i++){
            sb.append(columnNames.get(i));
            if(i<columnNames.size() -1){
                sb.append(", ");
            } else {
                sb.append(")");
            }
        }
        sb.append(" VALUES (");
        for(int i = 0 ; i<columnNames.size(); i++){
            sb.append("?");
            if(i<columnNames.size() -1){
                sb.append(", ");
            } else {
                sb.append(")");
            }
        }

        return conn.prepareStatement(sb.toString());
    }

    protected static String normalizeTablename(String tablename){
        String ret = tablename;
        if(tablename.startsWith("qrtz")){
            ret = tablename.toUpperCase();
        }
        if(tablename.equals("clusterhostmapping")){
            ret = "ClusterHostMapping";
        }
        return ret;
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
