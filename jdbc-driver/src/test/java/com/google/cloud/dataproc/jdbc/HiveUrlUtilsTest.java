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

import com.google.cloud.dataproc.jdbc.HiveJdbcConnectionOptions.Builder;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class HiveUrlUtilsTest {

    private final int PORT = 10003;
    private final String DB_NAME = "db-name";
    private final String HOST = "simple-cluster-m";

    @Test
    public void parseHiveUrl_simple() throws InvalidURLException {
        String simpleUrl = "jdbc:dataproc://hive/;clusterName=simple-cluster";
        String hiveUrl = "jdbc:hive2://simple-cluster-m:10001/;transportMode=http;httpPath=cliservice";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(simpleUrl);
        assertThat(param).isEqualTo(HiveJdbcConnectionOptions.builder().setHost(HOST).build());
        assertThat(param.toHiveJdbcUrl()).isEqualTo(hiveUrl);
    }

    @Test
    public void parseHiveUrl_simpleDuplicate_NotAccepted() {
        String simpleUrlDuplicate =
                "jdbc:dataproc://hive/;httpPath=hive;transportMode=http;clusterName=simple-cluster";
        Assertions.assertThrows(
                InvalidURLException.class,
                () -> {
                    HiveUrlUtils.parseHiveUrl(simpleUrlDuplicate);
                });
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
                "jdbc:dataproc://hive/db-name;port=10003;clusterName=simple-cluster";
        String hiveUrl =
                "jdbc:hive2://simple-cluster-m:10003/db-name;transportMode=http;httpPath=cliservice";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(simpleUrlWithDbAndPort);
        assertThat(param).isEqualTo(HiveJdbcConnectionOptions.builder().setHost(HOST).setDbName(DB_NAME).setPort(PORT).build());
        assertThat(param.toHiveJdbcUrl()).isEqualTo(hiveUrl);
    }

    @Test
    public void parseHiveUrl_longWithOtherSessionConfs() throws InvalidURLException {
        String longUrlWithOtherSessionConfs =
                "jdbc:dataproc://hive/db-name;port=10003;clusterName=simple-cluster;user=foo;password=bar";
        String hiveUrl =
                "jdbc:hive2://simple-cluster-m:10003/db-name;transportMode=http;httpPath=cliservice;user=foo;password=bar";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(longUrlWithOtherSessionConfs);
        assertThat(param).isEqualTo(HiveJdbcConnectionOptions.builder()
                .setHost(HOST)
                .setDbName(DB_NAME)
                .setPort(PORT)
                .setOtherSessionConfs("user=foo;password=bar")
                .build());
        assertThat(param.toHiveJdbcUrl()).isEqualTo(hiveUrl);
    }

    @Test
    public void parseHiveUrl_longWithHiveConfs() throws InvalidURLException {
        String longUrlWithHiveConfs =
                "jdbc:dataproc://hive/db-name;port=10003;clusterName=simple-cluster?hive.support.concurrency=true";
        String hiveUrl =
                "jdbc:hive2://simple-cluster-m:10003/db-name;transportMode=http;httpPath=cliservice?hive.support.concurrency=true";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(longUrlWithHiveConfs);
        assertThat(param).isEqualTo(HiveJdbcConnectionOptions.builder()
                .setHost(HOST)
                .setDbName(DB_NAME)
                .setPort(PORT)
                .setHiveConfs("hive.support.concurrency=true")
                .build());
        assertThat(param.toHiveJdbcUrl()).isEqualTo(hiveUrl);
    }

    @Test
    public void parseHiveUrl_longWithHiveVars() throws InvalidURLException {
        String longUrlWithHiveVars =
                "jdbc:dataproc://hive/db-name;port=10003;clusterName=simple-cluster#a=123";
        String hiveUrl =
                "jdbc:hive2://simple-cluster-m:10003/db-name;transportMode=http;httpPath=cliservice#a=123";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(longUrlWithHiveVars);
        assertThat(param).isEqualTo(HiveJdbcConnectionOptions.builder()
                .setHost(HOST)
                .setDbName(DB_NAME)
                .setPort(PORT)
                .setHiveVars("a=123")
                .build());
        assertThat(param.toHiveJdbcUrl()).isEqualTo(hiveUrl);
    }

    @Test
    public void parseHiveUrl_longComplete() throws InvalidURLException {
        String longUrlWithHiveVars =
                "jdbc:dataproc://hive/db-name;port=10003;clusterName=simple-cluster;user=foo;password=bar?hive.support.concurrency=true#a=123";
        String hiveUrl =
                "jdbc:hive2://simple-cluster-m:10003/db-name;transportMode=http;httpPath=cliservice;user=foo;password=bar?hive.support.concurrency=true#a=123";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(longUrlWithHiveVars);
        assertThat(param).isEqualTo(HiveJdbcConnectionOptions.builder()
                .setHost(HOST)
                .setDbName(DB_NAME)
                .setPort(PORT)
                .setOtherSessionConfs("user=foo;password=bar")
                .setHiveConfs("hive.support.concurrency=true")
                .setHiveVars("a=123")
                .build());
        assertThat(param.toHiveJdbcUrl()).isEqualTo(hiveUrl);
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
}