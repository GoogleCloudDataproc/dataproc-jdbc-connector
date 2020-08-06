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

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.dataproc.v1beta2.Cluster;
import com.google.cloud.dataproc.v1beta2.ClusterConfig;
import com.google.cloud.dataproc.v1beta2.ClusterControllerClient;
import com.google.cloud.dataproc.v1beta2.ClusterOperationMetadata;
import com.google.cloud.dataproc.v1beta2.EndpointConfig;
import com.google.cloud.dataproc.v1beta2.InstanceGroupConfig;
import com.google.cloud.dataproc.v1beta2.NodeInitializationAction;
import com.google.protobuf.Empty;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/** Helper class for system test that contains utils method of cluster creation and deletion. */
public class SystemTestSetUp {
    public static void createCluster(
            ClusterControllerClient clusterControllerClient,
            String projectId,
            String region,
            String clusterName,
            Map<String, String> clusterLabel)
            throws InterruptedException, ExecutionException {
        System.out.println(String.format("Start creating cluster: %s", clusterName));

        // Configure the settings for our cluster.
        InstanceGroupConfig masterConfig =
                InstanceGroupConfig.newBuilder()
                        .setMachineTypeUri("n1-standard-1")
                        .setNumInstances(1)
                        .build();
        InstanceGroupConfig workerConfig =
                InstanceGroupConfig.newBuilder()
                        .setMachineTypeUri("n1-standard-1")
                        .setNumInstances(2)
                        .build();

        // init action that sets Hive on the cluster to be running in HTTP mode
        NodeInitializationAction action1 =
                NodeInitializationAction.newBuilder()
                        .setExecutableFile("gs://hive-http-mode-init-action/hive-http-config.sh")
                        .build();
        // init action that prepares input file on the cluster at /tmp/input.txt
        NodeInitializationAction action2 =
                NodeInitializationAction.newBuilder()
                        .setExecutableFile("gs://hive-http-mode-init-action/system-test-setup.sh")
                        .build();

        EndpointConfig endpointConfig =
                EndpointConfig.newBuilder().setEnableHttpPortAccess(true).build();
        ClusterConfig clusterConfig =
                ClusterConfig.newBuilder()
                        .setMasterConfig(masterConfig)
                        .setWorkerConfig(workerConfig)
                        .setEndpointConfig(endpointConfig)
                        .addInitializationActions(action1)
                        .addInitializationActions(action2)
                        .build();

        // Create the cluster object with the desired cluster config.
        Cluster cluster =
                Cluster.newBuilder()
                        .setClusterName(clusterName)
                        .setConfig(clusterConfig)
                        .putAllLabels(clusterLabel)
                        .build();

        // Create the Cloud Dataproc cluster.
        OperationFuture<Cluster, ClusterOperationMetadata> createClusterAsyncRequest =
                clusterControllerClient.createClusterAsync(projectId, region, cluster);
        Cluster response = createClusterAsyncRequest.get();
        System.out.println(
                String.format("Cluster created successfully: %s\n", response.getClusterName()));
    }

    public static void deleteCluster(
            ClusterControllerClient clusterControllerClient,
            String projectId,
            String region,
            String clusterName)
            throws InterruptedException, ExecutionException {
        System.out.println(String.format("Start deleting cluster: %s", clusterName));
        // Delete the cluster.
        OperationFuture<Empty, ClusterOperationMetadata> deleteClusterAsyncRequest =
                clusterControllerClient.deleteClusterAsync(projectId, region, clusterName);
        deleteClusterAsyncRequest.get();
        System.out.println(String.format("Cluster \"%s\" successfully deleted.\n", clusterName));
    }
}