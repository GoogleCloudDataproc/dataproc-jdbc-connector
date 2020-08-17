/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.dataproc.jdbc.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

/** Example client usage of the Dataproc JDBC Driver. */
public class DataprocClient {

    public static String validateHost(String jdbcUrl, Scanner in, Properties... info)
            throws SQLException {
        Connection con = DriverManager.getConnection(jdbcUrl);
        System.out.println("\nConnection established.\n");
        Statement stmt = con.createStatement();

        String tableName = "testHiveDriverTable";

        stmt.execute("drop table if exists " + tableName);
        stmt.execute(
                "create table "
                        + tableName
                        + " (key int, value string, hostname string) row format delimited fields terminated by"
                        + " ','");

        // Show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }

        // Describe table
        sql = "describe " + tableName;
        System.out.println("\nRunning: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }

        // Load data into table
        // NOTE: filepath has to be local to the hive server
        // NOTE: /tmp/input.txt is a comma separated file with two fields per line
        String filepath = "/tmp/input.txt";
        sql = "load data local inpath '" + filepath + "' into table " + tableName;
        System.out.println("\nRunning: " + sql);
        stmt.execute(sql);

        // select * query
        sql = "select hostname from " + tableName + " where key = 1";
        System.out.println("\nRunning: " + sql);
        res = stmt.executeQuery(sql);
        String host = null;
        if (res.next()) {
            host = res.getString(1);
            System.out.println("Host name is: " + host);
        }

        System.out.println("\nDo you want to execute another query? [y/n]");
        String response = in.nextLine();
        while (response.equalsIgnoreCase("y")) {
            System.out.println("\nPlease enter the SQL command: ");
            sql = in.nextLine();
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
            System.out.println("\nRunning: " + sql);
            res = stmt.executeQuery(sql);
            ResultSetMetaData rsmd = res.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            if (res.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = res.getString(i);
                    System.out.print(columnValue + " " + rsmd.getColumnName(i));
                }
                System.out.println();
            }
            System.out.println("\nDo you want to execute another query? [y/n]");
            response = in.nextLine();
        }

        stmt.close();
        con.close();
        System.out.println("Connection closed.\n");
        return host;
    }

    public static void main(String[] args) throws SQLException {
        System.out.println("Demo for DataprocDriver...\n");
        Scanner in = new Scanner(System.in);

        System.out.println("Please provide the connection url string: ");
        String url = in.nextLine();

        validateHost(url, in);

        System.out.println("Do you want to connect again? [y/n]");
        String response = in.nextLine();

        while (response.equalsIgnoreCase("y")) {
            System.out.println("Please provide the connection url string: ");
            url = in.nextLine();
            validateHost(url, in);
            System.out.println("Do you want to connect again? [y/n]");
            response = in.nextLine();
        }

        in.close();
    }
}