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

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class HiveUrlUtilsTest {
    private final int PORT = 10003;
    private final String DB_NAME = "db-name";
    private final String CLUSTER_NAME = "simple-cluster";
    private final String PROJECT_ID = "pid";
    private final String REGION = "us-central1";

    @Test
    public void parseHiveUrl_simple() throws InvalidURLException {
        String simpleUrl =
                "jdbc:dataproc://hive/;projectId=pid;region=us-central1;clusterName=simple-cluster";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(simpleUrl);
        assertThat(param)
                .isEqualTo(
                        HiveJdbcConnectionOptions.builder()
                                .setProjectId(PROJECT_ID)
                                .setRegion(REGION)
                                .setClusterName(CLUSTER_NAME)
                                .build());
    }

    @Test
    public void parseHiveUrl_simpleDuplicate_notAccepted() {
        String simpleUrlDuplicate =
                "jdbc:dataproc://hive/;httpPath=hive;transportMode=http;projectId=pid;region=us-central1;clusterName=simple-cluster";
        Assertions.assertThrows(
                InvalidURLException.class,
                () -> {
                    HiveUrlUtils.parseHiveUrl(simpleUrlDuplicate);
                });
    }

    @Test
    public void parseHiveUrl_simpleNoClusterName_isAccepted() throws InvalidURLException {
        String simpleUrl = "jdbc:dataproc://hive/;projectId=pid;region=us-central1";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(simpleUrl);
        assertThat(param)
                .isEqualTo(
                        HiveJdbcConnectionOptions.builder().setProjectId(PROJECT_ID).setRegion(REGION).build());
    }

    @Test
    public void parseHiveUrl_simpleWithLabel() throws InvalidURLException {
        String simpleUrl =
                "jdbc:dataproc://hive/;projectId=pid;region=us-central1;clusterPoolLabel=com=google";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(simpleUrl);
        assertThat(param)
                .isEqualTo(
                        HiveJdbcConnectionOptions.builder()
                                .setProjectId(PROJECT_ID)
                                .setRegion(REGION)
                                .setClusterPoolLabel("com=google")
                                .build());
    }

    @Test
    public void parseHiveUrl_simpleIncomplete() {
        String simpleUrlIncomplete = "jdbc:dataproc://hive/;";
        Assertions.assertThrows(
                InvalidURLException.class,
                () -> {
                    HiveUrlUtils.parseHiveUrl(simpleUrlIncomplete);
                });
    }

    @Test
    public void parseHiveUrl_simpleWithDbAndPort() throws InvalidURLException {
        String simpleUrlWithDbAndPort =
                "jdbc:dataproc://hive/db-name;port=10003;projectId=pid;region=us-central1;clusterName=simple-cluster";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(simpleUrlWithDbAndPort);
        assertThat(param)
                .isEqualTo(
                        HiveJdbcConnectionOptions.builder()
                                .setProjectId(PROJECT_ID)
                                .setRegion(REGION)
                                .setClusterName(CLUSTER_NAME)
                                .setDbName(DB_NAME)
                                .setPort(PORT)
                                .build());
    }

    @Test
    public void parseHiveUrl_longWithOtherSessionConfs() throws InvalidURLException {
        String longUrlWithOtherSessionConfs =
                "jdbc:dataproc://hive/db-name;port=10003;projectId=pid;region=us-central1;clusterName=simple-cluster;user=foo;password=bar";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(longUrlWithOtherSessionConfs);
        assertThat(param)
                .isEqualTo(
                        HiveJdbcConnectionOptions.builder()
                                .setProjectId(PROJECT_ID)
                                .setRegion(REGION)
                                .setClusterName(CLUSTER_NAME)
                                .setDbName(DB_NAME)
                                .setPort(PORT)
                                .setOtherSessionConfs("user=foo;password=bar")
                                .build());
    }

    @Test
    public void parseHiveUrl_longLabel() throws InvalidURLException {
        String simpleUrl =
                "jdbc:dataproc://hive/;projectId=pid;region=us-central1;clusterPoolLabel=com=google:group=test";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(simpleUrl);
        assertThat(param)
                .isEqualTo(
                        HiveJdbcConnectionOptions.builder()
                                .setProjectId(PROJECT_ID)
                                .setRegion(REGION)
                                .setClusterPoolLabel("com=google:group=test")
                                .build());
    }

    @Test
    public void parseHiveUrl_longWithHiveConfs() throws InvalidURLException {
        String longUrlWithHiveConfs =
                "jdbc:dataproc://hive/db-name;port=10003;projectId=pid;region=us-central1;clusterName=simple-cluster?hive.support.concurrency=true";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(longUrlWithHiveConfs);
        assertThat(param)
                .isEqualTo(
                        HiveJdbcConnectionOptions.builder()
                                .setProjectId(PROJECT_ID)
                                .setRegion(REGION)
                                .setClusterName(CLUSTER_NAME)
                                .setDbName(DB_NAME)
                                .setPort(PORT)
                                .setHiveConfs("hive.support.concurrency=true")
                                .build());
    }

    @Test
    public void parseHiveUrl_longWithHiveVars() throws InvalidURLException {
        String longUrlWithHiveVars =
                "jdbc:dataproc://hive/db-name;port=10003;projectId=pid;region=us-central1;clusterName=simple-cluster#a=123";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(longUrlWithHiveVars);
        assertThat(param)
                .isEqualTo(
                        HiveJdbcConnectionOptions.builder()
                                .setProjectId(PROJECT_ID)
                                .setRegion(REGION)
                                .setClusterName(CLUSTER_NAME)
                                .setDbName(DB_NAME)
                                .setPort(PORT)
                                .setHiveVars("a=123")
                                .build());
    }

    @Test
    public void parseHiveUrl_longComplete() throws InvalidURLException {
        String longUrlComplete =
                "jdbc:dataproc://hive/db-name;port=10003;projectId=pid;region=us-central1;clusterName=simple-cluster;user=foo;password=bar?hive.support.concurrency=true#a=123";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(longUrlComplete);
        assertThat(param)
                .isEqualTo(
                        HiveJdbcConnectionOptions.builder()
                                .setProjectId(PROJECT_ID)
                                .setRegion(REGION)
                                .setClusterName(CLUSTER_NAME)
                                .setDbName(DB_NAME)
                                .setPort(PORT)
                                .setOtherSessionConfs("user=foo;password=bar")
                                .setHiveConfs("hive.support.concurrency=true")
                                .setHiveVars("a=123")
                                .build());
    }

    @Test
    public void parseHiveUrl_longCompleteWithLabel() throws InvalidURLException {
        String longUrlWithLabel =
                "jdbc:dataproc://hive/db-name;port=10003;projectId=pid;region=us-central1;clusterPoolLabel=com=google:group=test;user=foo;password=bar?hive.support.concurrency=true#a=123";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(longUrlWithLabel);
        assertThat(param)
                .isEqualTo(
                        HiveJdbcConnectionOptions.builder()
                                .setProjectId(PROJECT_ID)
                                .setRegion(REGION)
                                .setClusterPoolLabel("com=google:group=test")
                                .setDbName(DB_NAME)
                                .setPort(PORT)
                                .setOtherSessionConfs("user=foo;password=bar")
                                .setHiveConfs("hive.support.concurrency=true")
                                .setHiveVars("a=123")
                                .build());
    }

    @Test
    public void parseHiveUrl_longInComplete() {
        String simpleUrlIncomplete =
                "jdbc:dataproc://hive/db-name;port=10003;user=hive?hive.support.concurrency=true#a=123";
        Assertions.assertThrows(
                InvalidURLException.class,
                () -> {
                    HiveUrlUtils.parseHiveUrl(simpleUrlIncomplete);
                });
    }

    @Test
    public void parseHiveUrl_noClusterInfo() {
        String urlNoClusterInfo = "jdbc:dataproc://hive/db-name;foo=bar";
        Assertions.assertThrows(
                InvalidURLException.class,
                () -> {
                    HiveUrlUtils.parseHiveUrl(urlNoClusterInfo);
                });
    }

    @Test
    public void parseHiveUrl_incorrectSessionConfs() {
        String urlNoClusterInfo = "jdbc:dataproc://hive/db-name;clusterName=simple-cluster;foo;bar";
        Assertions.assertThrows(
                InvalidURLException.class,
                () -> {
                    HiveUrlUtils.parseHiveUrl(urlNoClusterInfo);
                });
    }

    @Test
    public void parseHiveUrl_clusterPoolIncomplete_missingRegion() {
        String urlNoClusterInfo =
                "jdbc:dataproc://hive/db-name;projectId=pid;clusterPoolLabel=clusterName=simple-cluster";
        Assertions.assertThrows(
                InvalidURLException.class,
                () -> {
                    HiveUrlUtils.parseHiveUrl(urlNoClusterInfo);
                });
    }
}