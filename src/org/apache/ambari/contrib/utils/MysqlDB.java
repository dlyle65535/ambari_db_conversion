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
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=0");
            stmt.close();
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
        sb.append(" values (");
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

    private String normalizeTablename(String tablename){
        String ret = tablename;
        if(tablename.startsWith("qrtz")){
            ret = tablename.toUpperCase();
        }
        if(tablename.equals("clusterhostmapping")){
            ret = "ClusterHostMapping";
        }
        return ret;
    }
}
