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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.dataproc.v1beta2.Cluster;
import com.google.cloud.dataproc.v1beta2.ClusterControllerClient;
import com.google.cloud.dataproc.v1beta2.ClusterStatus;

import java.sql.SQLException;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class DataprocInfoTest {
    private final String PROJECT_ID = "pid";
    private final String REGION = "us-central1";
    private final String FILTER_DUPLICATE = "status.state = ACTIVE AND clusterName = simple-cluster1";
    private final String FILTER_DEFAULT = "status.state = ACTIVE";
    private final String FILTER_LONG =
            "status.state = ACTIVE AND labels.com = google AND labels.env = staging AND labels.team ="
                    + " dataproc";
    private final String CLUSTER_NAME_1 = "simple-cluster1";
    private final String CLUSTER_NAME_2 = "simple-cluster2";
    private final String NO_CLUSTER_NAME = "no-cluster";

    private HiveJdbcConnectionOptions.Builder paramBuilder;
    private DataprocInfo infoTest;

    // Mock CluterControllerClient
    private ClusterControllerClient mockClusterControllerClient;

    // Mock ListClustersPagedResponse
    private ClusterControllerClient.ListClustersPagedResponse mockListDefault;
    private ClusterControllerClient.ListClustersPagedResponse mockListDefaultDup;

    // Clusters
    private Cluster cluster1;
    private Cluster cluster2;

    @Before
    public void setUp() {
        mockClusterControllerClient = mock(ClusterControllerClient.class);
        mockListDefault = mock(ClusterControllerClient.ListClustersPagedResponse.class);
        mockListDefaultDup = mock(ClusterControllerClient.ListClustersPagedResponse.class);

        cluster1 =
                Cluster.newBuilder()
                        .setClusterName(CLUSTER_NAME_1)
                        .setProjectId(PROJECT_ID)
                        .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.RUNNING).build())
                        .build();
        cluster2 =
                Cluster.newBuilder()
                        .setClusterName(CLUSTER_NAME_2)
                        .setProjectId(PROJECT_ID)
                        .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.RUNNING).build())
                        .build();

        when(mockListDefault.iterateAll())
                .thenReturn(Collections.singleton(cluster2))
                .thenReturn(Collections.singleton(cluster1));
        when(mockListDefaultDup.iterateAll())
                .thenReturn(Collections.singleton(cluster1))
                .thenReturn(Collections.singleton(cluster2));
        when(mockClusterControllerClient.listClusters(PROJECT_ID, REGION, FILTER_DEFAULT))
                .thenReturn(mockListDefault);
        when(mockClusterControllerClient.listClusters(PROJECT_ID, REGION, FILTER_DUPLICATE))
                .thenReturn(mockListDefaultDup);
        when(mockClusterControllerClient.listClusters(PROJECT_ID, REGION, FILTER_LONG))
                .thenReturn(mockListDefaultDup);

        when(mockClusterControllerClient.getCluster(PROJECT_ID, REGION, CLUSTER_NAME_1))
                .thenReturn(cluster1);
        when(mockClusterControllerClient.getCluster(PROJECT_ID, REGION, CLUSTER_NAME_2))
                .thenReturn(cluster2);
        when(mockClusterControllerClient.getCluster(PROJECT_ID, REGION, NO_CLUSTER_NAME))
                .thenThrow(
                        new ApiException(
                                new Throwable("io.grpc.StatusRuntimeException: NOT_FOUND"),
                                new StatusCode() {
                                    @Override
                                    public Code getCode() {
                                        return Code.NOT_FOUND;
                                    }

                                    @Override
                                    public Object getTransportCode() {
                                        return null;
                                    }
                                },
                                false));

        paramBuilder = HiveJdbcConnectionOptions.builder().setProjectId(PROJECT_ID).setRegion(REGION);
    }

    @Test
    public void formatClusterFilterString_noFilter_isCorrect() throws InvalidURLException {
        String clusterPoolLabel = null;
        HiveJdbcConnectionOptions param = paramBuilder.setClusterPoolLabel(clusterPoolLabel).build();
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        assertThat(infoTest.formatClusterFilterString()).isEqualTo(FILTER_DEFAULT);
    }

    @Test
    public void formatClusterFilterString_longFilter() throws InvalidURLException {
        String clusterPoolLabel = "com=google:env=staging:team=dataproc";
        HiveJdbcConnectionOptions param = paramBuilder.setClusterPoolLabel(clusterPoolLabel).build();
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        assertThat(infoTest.formatClusterFilterString()).isEqualTo(FILTER_LONG);
    }

    @Test
    public void formatClusterFilterString_duplicateStatusFilter_inCorrect() {
        String clusterPoolLabel = "status.state=ACTIVE";
        HiveJdbcConnectionOptions param = paramBuilder.setClusterPoolLabel(clusterPoolLabel).build();
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        Assertions.assertThrows(
                InvalidURLException.class,
                () -> {
                    infoTest.formatClusterFilterString();
                });
    }

    @Test
    public void formatClusterFilterString_invalidLabel_inCorrect() {
        String clusterPoolLabel = "com=google:foo:env=staging";
        HiveJdbcConnectionOptions param = paramBuilder.setClusterPoolLabel(clusterPoolLabel).build();
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        Assertions.assertThrows(
                InvalidURLException.class,
                () -> {
                    infoTest.formatClusterFilterString();
                });
    }

    @Test
    public void formatClusterFilterString_duplicateLabelKey_inCorrect() {
        String clusterPoolLabel = "com=google:env=staging:com=random";
        HiveJdbcConnectionOptions param = paramBuilder.setClusterPoolLabel(clusterPoolLabel).build();
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        Assertions.assertThrows(
                InvalidURLException.class,
                () -> {
                    infoTest.formatClusterFilterString();
                });
    }

    @Test
    public void formatClusterFilterString_wrongFilter_inCorrect() {
        String clusterPoolLabel = "com&google";
        HiveJdbcConnectionOptions param = paramBuilder.setClusterPoolLabel(clusterPoolLabel).build();
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        Assertions.assertThrows(
                InvalidURLException.class,
                () -> {
                    infoTest.formatClusterFilterString();
                });
    }

    @Test
    public void getHost_clusterName() throws SQLException {
        HiveJdbcConnectionOptions param = paramBuilder.setClusterName(CLUSTER_NAME_1).build();
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        assertThat(infoTest.getHost()).isEqualTo(CLUSTER_NAME_1 + "-m");
    }

    @Test
    public void getHost_noClusterFoundByName() {
        HiveJdbcConnectionOptions param = paramBuilder.setClusterName(NO_CLUSTER_NAME).build();
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        Assertions.assertThrows(
                InvalidURLException.class,
                () -> {
                    infoTest.getHost();
                });
    }

    @Test
    public void getHost_defaultLabel() throws SQLException {
        String clusterPoolLabel = null;
        HiveJdbcConnectionOptions param = paramBuilder.setClusterPoolLabel(clusterPoolLabel).build();
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        assertThat(infoTest.getHost()).isEqualTo(CLUSTER_NAME_2 + "-m");
    }

    @Test
    public void getHost_simpleClusterLabel() throws SQLException {
        String clusterPoolLabel = "clusterName=simple-cluster1";
        HiveJdbcConnectionOptions param = paramBuilder.setClusterPoolLabel(clusterPoolLabel).build();
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        assertThat(infoTest.getHost()).isEqualTo(CLUSTER_NAME_1 + "-m");
    }

    @Test
    public void getHost_longClusterLabel() throws SQLException {
        String clusterPoolLabel = "com=google:env=staging:team=dataproc";
        HiveJdbcConnectionOptions param = paramBuilder.setClusterPoolLabel(clusterPoolLabel).build();
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        assertThat(infoTest.getHost()).isEqualTo(CLUSTER_NAME_1 + "-m");
    }

    @Test
    public void toHiveJdbcUrl_simpleClusterName() throws SQLException {
        String simpleUrl =
                "jdbc:dataproc://hive/;projectId=pid;region=us-central1;clusterName=simple-cluster1";
        String hiveUrl = "jdbc:hive2://simple-cluster1-m:10001/;transportMode=http;httpPath=cliservice";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(simpleUrl);
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        assertThat(infoTest.toHiveJdbcUrl()).isEqualTo(hiveUrl);
    }

    @Test
    public void toHiveJdbcUrl_simpleClusterLabel() throws SQLException {
        String simpleUrl =
                "jdbc:dataproc://hive/;projectId=pid;region=us-central1;clusterPoolLabel=com=google:env=staging:team=dataproc";
        String hiveUrl = "jdbc:hive2://simple-cluster1-m:10001/;transportMode=http;httpPath=cliservice";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(simpleUrl);
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        assertThat(infoTest.toHiveJdbcUrl()).isEqualTo(hiveUrl);
    }

    @Test
    public void toHiveJdbcUrl_longClusterName() throws SQLException {
        String longUrlComplete =
                "jdbc:dataproc://hive/db-name;port=10003;projectId=pid;region=us-central1;clusterName=simple-cluster2;user=foo;password=bar?hive.support.concurrency=true#a=123";
        String hiveUrl =
                "jdbc:hive2://simple-cluster2-m:10003/db-name;transportMode=http;httpPath=cliservice;user=foo;password=bar?hive.support.concurrency=true#a=123";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(longUrlComplete);
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        assertThat(infoTest.toHiveJdbcUrl()).isEqualTo(hiveUrl);
    }

    @Test
    public void toHiveJdbcUrl_longClusterLabel() throws SQLException {
        String longUrlComplete =
                "jdbc:dataproc://hive/db-name;port=10003;projectId=pid;region=us-central1;clusterPoolLabel=com=google:env=staging:team=dataproc;user=foo;password=bar?hive.support.concurrency=true#a=123";
        String hiveUrl =
                "jdbc:hive2://simple-cluster1-m:10003/db-name;transportMode=http;httpPath=cliservice;user=foo;password=bar?hive.support.concurrency=true#a=123";
        HiveJdbcConnectionOptions param = HiveUrlUtils.parseHiveUrl(longUrlComplete);
        DataprocInfo infoTest = new DataprocInfo(param, mockClusterControllerClient);
        assertThat(infoTest.toHiveJdbcUrl()).isEqualTo(hiveUrl);
    }
}