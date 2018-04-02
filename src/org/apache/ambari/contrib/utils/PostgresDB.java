package org.apache.ambari.contrib.utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresDB {

    private Connection conn =  null;
    private Statement stmt = null;
    private Map<String,Long> rowCounts = new HashMap<>();
    private PostgresDB(){};

    public PostgresDB(String url, String user, String password) {
        try {
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to create database connection " + url);
        }
    }

    List<String> getTables(){
        List<String> ret = new ArrayList<>();
        try {
            DatabaseMetaData md = conn.getMetaData();
            String[] types = new String[]{"TABLE"};
            ResultSet rs = md.getTables(null, null, "%", types);
            for(int i=1; i< rs.getMetaData().getColumnCount()+1; i++){
                System.out.print(rs.getMetaData().getColumnName(i) + " ");
            }
            while(rs.next()){
                Object column = rs.getObject(3);
                ret.add(column.toString());
            }
            rs.close();
        } catch (SQLException e){
            e.printStackTrace();
        }
        return ret;
    }


    ResultSet getRows(String tablename){
       ResultSet rs = null;
        try {
            rs = stmt.executeQuery("SELECT * FROM " + tablename + ";");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }
}
