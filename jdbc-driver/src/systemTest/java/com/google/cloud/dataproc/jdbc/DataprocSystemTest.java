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
package com.google.cloud.dataproc.jdbc;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.dataproc.v1beta2.ClusterControllerClient;
import com.google.cloud.dataproc.v1beta2.ClusterControllerSettings;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

/**
 * This class tests end-to-end usage of the DataprocDriver. It tests the workflow for cluster
 * creation with init action and Component Gateway enabled, connection establishment using various
 * JDBC URL string, query execution with Hive, and cluster deletion after testing.
 */
public class DataprocSystemTest {
    private static final String REGION = System.getProperty("region");
    private static final String PROJECT_ID = System.getProperty("projectId");
    private static final String USER = System.getProperty("user");
    private static ClusterControllerClient clusterControllerClient;
    private static Map<String, Map<String, String>> labelsByClusterName;
    private static String cluster1;
    private static String cluster2;
    private static String cluster3;
    private static String cluster4;

    @BeforeClass
    public static void setUp() throws IOException, ExecutionException, InterruptedException {
        cluster1 = String.format("%s-test-cluster1", USER);
        cluster2 = String.format("%s-test-cluster2", USER);
        cluster3 = String.format("%s-test-cluster3", USER);
        cluster4 = String.format("%s-test-cluster4", USER);

        String myEndpoint = String.format("%s-dataproc.googleapis.com:443", REGION);
        ClusterControllerSettings clusterControllerSettings =
                ClusterControllerSettings.newBuilder().setEndpoint(myEndpoint).build();
        clusterControllerClient = ClusterControllerClient.create(clusterControllerSettings);

        // Clean up leaked clusters for the same user as indicated by USER prefix before running
        deleteClusters();

        labelsByClusterName = new HashMap<>();
        Map<String, String> clusterLabel1 =
                ImmutableMap.of("com", "google", "team", "dataproc", "env", "staging");
        SystemTestSetUp.createCluster(
                clusterControllerClient, PROJECT_ID, REGION, cluster1, clusterLabel1);
        labelsByClusterName.put(cluster1 + "-m", clusterLabel1);

        Map<String, String> clusterLabel2 =
                ImmutableMap.of("com", "google", "team", "dataproc", "customer", "test");
        SystemTestSetUp.createCluster(
                clusterControllerClient, PROJECT_ID, REGION, cluster2, clusterLabel1);
        labelsByClusterName.put(cluster2 + "-m", clusterLabel2);

        Map<String, String> clusterLabel3 =
                ImmutableMap.of("com", "google", "team", "dataproc", "customer", "test");
        SystemTestSetUp.createCluster(
                clusterControllerClient, PROJECT_ID, REGION, cluster3, clusterLabel1);
        labelsByClusterName.put(cluster3 + "-m", clusterLabel3);

        Map<String, String> clusterLabel4 =
                ImmutableMap.of("com", "google", "team", "tmp", "customer", "test");
        SystemTestSetUp.createCluster(
                clusterControllerClient, PROJECT_ID, REGION, cluster4, clusterLabel4);
        labelsByClusterName.put(cluster4 + "-m", clusterLabel4);
    }

    public static void printErrorMessage(Exception e, String clusterName) {
        if (e.getCause() instanceof NotFoundException) {
            System.out.println(String.format("No cluster to delete for %s.\n", clusterName));
        } else {
            System.out.println(
                    String.format("Failed to delete cluster %s.\n", clusterName, e.getMessage()));
        }
    }

    public static void deleteClusters() {
        try {
            SystemTestSetUp.deleteCluster(clusterControllerClient, PROJECT_ID, REGION, cluster1);
        } catch (ExecutionException | InterruptedException e) {
            printErrorMessage(e, cluster1);
        }

        try {
            SystemTestSetUp.deleteCluster(clusterControllerClient, PROJECT_ID, REGION, cluster2);
        } catch (ExecutionException | InterruptedException e) {
            printErrorMessage(e, cluster2);
        }

        try {
            SystemTestSetUp.deleteCluster(clusterControllerClient, PROJECT_ID, REGION, cluster3);
        } catch (ExecutionException | InterruptedException e) {
            printErrorMessage(e, cluster3);
        }

        try {
            SystemTestSetUp.deleteCluster(clusterControllerClient, PROJECT_ID, REGION, cluster4);
        } catch (ExecutionException | InterruptedException e) {
            printErrorMessage(e, cluster4);
        }
    }

    @AfterClass
    public static void cleanUp() {
        deleteClusters();
        clusterControllerClient.close();
    }

    public void loadTable(Statement stmt, String tableName) throws SQLException {
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
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }

        // Load data into table
        // NOTE: /tmp/input.txt is a comma separated file with two fields per line,
        //       comes from the init action and is populated with the master VM's hostname
        String filepath = "/tmp/input.txt";
        sql = "load data local inpath '" + filepath + "' into table " + tableName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    public String extractHostnameFromTable(Statement stmt, String tableName) throws SQLException {
        // Select * query
        String sql = "select hostname from " + tableName + " where key = 1";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        String host = null;
        if (res.next()) {
            host = res.getString(1);
            System.out.println("Host name is: " + host);
        }
        return host;
    }

    public String getHostName(String jdbcUrl, Properties... info) throws SQLException {
        Connection con = DriverManager.getConnection(jdbcUrl);
        System.out.println("Connection established.");
        Statement stmt = con.createStatement();
        String tableName = "testHiveDriverTable";

        loadTable(stmt, tableName);
        String host = extractHostnameFromTable(stmt, tableName);

        stmt.close();
        con.close();
        return host;
    }

    @Test
    public void testClusterName() throws SQLException {
        String url =
                String.format(
                        "jdbc:dataproc://hive/;projectId=%s;region=%s;clusterName=%s",
                        PROJECT_ID, REGION, cluster2);
        assertThat(getHostName(url)).isEqualTo(cluster2 + "-m");
    }

    @Test
    public void testClusterLabel() throws SQLException {
        String url =
                String.format(
                        "jdbc:dataproc://hive/;projectId=%s;region=%s;clusterPoolLabel=com=google:team=dataproc",
                        PROJECT_ID, REGION);
        String host = getHostName(url);

        assertTrue(labelsByClusterName.get(host).get("com").equals("google"));
        assertTrue(labelsByClusterName.get(host).get("team").equals("dataproc"));
    }

    @Test
    public void testClusterName_notExist() {
        String url =
                String.format(
                        "jdbc:dataproc://hive/;projectId=%s;region=%s;clusterName=test-not-exist",
                        PROJECT_ID, REGION);
        Assertions.assertThrows(
                SQLException.class,
                () -> {
                    getHostName(url);
                });
    }

    @Test
    public void testClusterLabel_wrongFormat() {
        String url =
                String.format(
                        "jdbc:dataproc://hive/;projectId=%s;region=%s;clusterPoolLabel=test-not-exist",
                        PROJECT_ID, REGION);
        Assertions.assertThrows(
                SQLException.class,
                () -> {
                    getHostName(url);
                });
    }

    @Test
    public void testClusterLabel_notExist() {
        String url =
                String.format(
                        "jdbc:dataproc://hive/;projectId=%s;region=%s;clusterPoolLabel=com=not-exist",
                        PROJECT_ID, REGION);
        Assertions.assertThrows(
                SQLException.class,
                () -> {
                    getHostName(url);
                });
    }
}