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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.*;

public class AmbariDBConvert {


    public static void main(String... argv) throws SQLException {


        Options options = buildOptions();
        Options help = new Options();
        Option o = new Option("h", "help", false, "This screen");
        o.setRequired(false);
        help.addOption(o);
        try {

            CommandLine cmd = checkOptions(help, options, argv);

            String psql_url = cmd.getOptionValue("p");
            String msql_url = cmd.getOptionValue("m");
            String username = cmd.getOptionValue("u");
            String password = cmd.getOptionValue("P");
            Class.forName("org.mariadb.jdbc.Driver");
            Class.forName("org.postgresql.Driver");
            MysqlDB mysql = new MysqlDB(msql_url,username, password);
            PostgresDB psql = new PostgresDB(psql_url, username, password);

            AmbariDBConvert converter = new AmbariDBConvert();
            converter.convert(psql, mysql);

        } catch(Exception e){
            e.printStackTrace();
            System.exit(-1);
        }

    }

    public static Options buildOptions(){

        Options options = new Options();

        Option o = new Option("p", "psql-url", true, "Postgres URL (e.g. jdbc:postgresql://postgres.host.example.com:5432/ambari");
        o.setArgName("PSQL_URL");
        o.setRequired(true);
        options.addOption(o);

        o = new Option("m", "mysql-url", true, "MySQL (MariaDB) URL (e.g. jdbc:mariadb://mysql.host.example.com:3306/ambari");
        o.setArgName("MYSQL_URL");
        o.setRequired(true);
        options.addOption(o);

        o = new Option("u", "username", true, "Database username (should be common for both databases)");
        o.setArgName("USERNAME");
        o.setRequired(true);
        options.addOption(o);

        o = new Option("P", "password", true, "Database password (should be common for both databases)");
        o.setArgName("PASSWORD");
        o.setRequired(true);
        options.addOption(o);

        return options;
    }

    public static CommandLine checkOptions(Options help, Options options, String ... argv) throws ParseException {

        CommandLine cmd = null;
        CommandLineParser parser = new DefaultParser();


        try {

            cmd = parser.parse(help,argv,true);

            if( cmd.getOptions().length > 0){
                final HelpFormatter usageFormatter = new HelpFormatter();
                usageFormatter.printHelp("AmbariDBConvert", null, options, null, true);
                System.exit(0);
            }

            cmd = parser.parse(options, argv);

        } catch (ParseException e) {

            final HelpFormatter usageFormatter = new HelpFormatter();
            usageFormatter.printHelp("AmbariDBConvert", null, options, null, true);
            throw e;

        }


        return cmd;
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
