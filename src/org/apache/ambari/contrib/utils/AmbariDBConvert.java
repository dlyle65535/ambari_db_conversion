package org.apache.ambari.contrib.utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class AmbariDBConvert {

    static final String PSQL_URL = "jdbc:postgresql://node1.hortonworks.example.com:5432/ambari";
    static final String MYSQL_URL = "jdbc:mariadb://node2.hortonworks.example.com:3306/ambari";

    public static void main(String[] args) throws SQLException {

        PostgresDB psql = new PostgresDB(PSQL_URL, "ambari", "bigdata");
        MysqlDB mysql = new MysqlDB(MYSQL_URL, "ambari", "bigdata");

        AmbariDBConvert converter = new AmbariDBConvert();
        converter.convert(psql,mysql);

    }

    private boolean convert(PostgresDB psql, MysqlDB mysql) throws SQLException {

        //TODO: Dump database and start again

        List<String> tables = psql.getTables();

        for (String table : tables){
            if(table.equals("key_value_store")){
                System.out.println("Skipping key_value_store");
                continue;
            }
            System.out.println("Deleteing from table: " + table);
            mysql.deleteFromTable(table);
            System.out.println("Getting rows from table: " + table);
            ResultSet rows = psql.getRows(table);
            ResultSetMetaData md = rows.getMetaData();
            int numCols = md.getColumnCount();
            PreparedStatement ps = mysql.buildInsertStatement(table,getColumnNames(md));
            while(rows.next()) {
                for (int i = 1; i < numCols + 1; i++) {

                    switch(md.getColumnType(i)){

                        case -7: //bit
                            ps.setBoolean(i,rows.getBoolean(i));
                            break;
                        case -6: //TINYINT
                            ps.setByte(i,rows.getByte(i));
                            break;
                        case -5: //BIGINT
                            ps.setLong(i,rows.getLong(i));
                            break;
                        case -4://LONGVARBINARY
                        case -3://VARBINARY
                        case -2://BINARY
                            ps.setBytes(i,rows.getBytes(i));
                            break;
                        case    -1://LONGVARCHAR
                        case 1://CHAR
                            ps.setString(i,rows.getString(i));
                            break;
                        case 0://NULL
                            ps.setNull(i,Types.NULL);
                            break;
                        case 2://NUMERIC
                        case 3://DECIMAL
                            ps.setBigDecimal(i,rows.getBigDecimal(i));
                            break;
                        case 4://INTEGER
                            ps.setInt(i,rows.getInt(i));
                            break;
                        case 5://SMALLINT
                            ps.setShort(i,rows.getShort(i));
                            break;
                        case 6://FLOAT
                        case 7://REAL
                            ps.setFloat(i,rows.getFloat(i));
                            break;
                        case 8://DOUBLE
                            ps.setDouble(i,rows.getDouble(i));
                            break;
                        case 12://VARCHAR
                            ps.setString(i,rows.getString(i));
                            break;
                        case 91://DATE
                            ps.setDate(i,rows.getDate(i));
                            break;
                        case 92://TIME
                            ps.setTime(i,rows.getTime(i));
                            break;
                        case 93: //TIMESTAMP
                            ps.setTimestamp(i,rows.getTimestamp(i));
                            break;
                        case 1111: //OTHER
                            throw new RuntimeException("Unable to map custom dataype from " + md.getColumnName(i) + " in table: " + table);
                    }

                }
                    ps.executeUpdate();
                          }
        }
        return true;
    }

    private List<String> getColumnNames(ResultSetMetaData md) throws SQLException {
        List<String> columnNames = new ArrayList<>();
        for(int i=1; i<md.getColumnCount()+1; i++){
            columnNames.add(md.getColumnName(i));
        }
        return columnNames;
    }
}
