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

import org.apache.commons.cli.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            AmbariDatabase mysql = new AmbariMySQLDatabase(msql_url, username, password);
            AmbariDatabase psql = new AmbariDatabase(psql_url, username, password);

            AmbariDBConvert converter = new AmbariDBConvert();
            converter.convert(psql, mysql);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }

    public static Options buildOptions() {

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

    public static CommandLine checkOptions(Options help, Options options, String... argv) throws ParseException {

        CommandLine cmd = null;
        CommandLineParser parser = new DefaultParser();


        try {

            cmd = parser.parse(help, argv, true);

            if (cmd.getOptions().length > 0) {
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

    protected boolean convert(AmbariDatabase sourceDB, AmbariDatabase destDB) {

        //TODO: Drop database and start again
        try {


            if(destDB instanceof AmbariMySQLDatabase) {
                ((AmbariMySQLDatabase) destDB).disableConstraints();
            }

            List<String> tables = sourceDB.getTables();
            Integer tableCount = 0;
            for (String table : tables) {
                tableCount++;
                System.out.println("Deleting from table: " + table);
                destDB.deleteFromTable(table);
                System.out.println("Getting rows from table: " + table);
                ResultSet rows = sourceDB.getRows(table);
                ResultSetMetaData md = rows.getMetaData();
                int numCols = md.getColumnCount();
                PreparedStatement ps = destDB.buildInsertStatement(table, getColumnNames(md));
                while (rows.next()) {
                    for (int i = 1; i < numCols + 1; i++) {

                        ps.setObject(i,rows.getObject(i));

                    }
                    try {
                        ps.executeUpdate();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("Copied " + tableCount + " tables.");
            if(destDB instanceof AmbariMySQLDatabase) {
                ((AmbariMySQLDatabase) destDB).enableConstraints();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true;
    }


    private List<String> getColumnNames(ResultSetMetaData md) throws SQLException {
        List<String> columnNames = new ArrayList<>();
        for (int i = 1; i < md.getColumnCount() + 1; i++) {
            columnNames.add(md.getColumnName(i));
        }
        return columnNames;
    }
}
