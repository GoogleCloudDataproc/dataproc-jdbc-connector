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

import static com.google.cloud.dataproc.jdbc.HiveUrlUtils.checkUrl;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.dataproc.v1beta2.Cluster;
import com.google.cloud.dataproc.v1beta2.ClusterControllerClient;
import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** Helper class to get cluster info through Dataproc API. */
public class DataprocInfo {
    private static final String HIVE_PROTOCOL = "jdbc:hive2://";
    private final ClusterControllerClient clusterControllerClient;
    private final HiveJdbcConnectionOptions params;

    // Constructor dependency injection
    public DataprocInfo(HiveJdbcConnectionOptions params, ClusterControllerClient controller) {
        this.params = params;
        this.clusterControllerClient = controller;
    }

    /**
     * Format client passed in url that is accepted by Dataproc to be Hive acceptable format.
     * jdbc:hive2://<host>:<port>/<dbName>;transportMode=http;httpPath=<http_endpoint>;<otherSessionConfs>?<hiveConfs>#<hiveVars>
     *
     * @return the formatted URL that can be accepted by Hive
     * @throws InvalidURLException
     */
    public String toHiveJdbcUrl() throws SQLException {
        String hiveJdbcURL =
                String.format(
                        "%s%s:%d/%s;transportMode=%s;httpPath=%s",
                        HIVE_PROTOCOL,
                        getHost(),
                        params.port(),
                        params.dbName(),
                        params.transportMode(),
                        params.httpPath());
        if (params.otherSessionConfs() != null) {
            hiveJdbcURL = String.format("%s;%s", hiveJdbcURL, params.otherSessionConfs());
        }
        if (params.hiveConfs() != null) {
            hiveJdbcURL = String.format("%s?%s", hiveJdbcURL, params.hiveConfs());
        }
        if (params.hiveVars() != null) {
            hiveJdbcURL = String.format("%s#%s", hiveJdbcURL, params.hiveVars());
        }
        return hiveJdbcURL;
    }

    /**
     * Use Dataproc client libraries to get cluster.
     *
     * @return name of the cluster's master node
     * @throws InvalidURLException
     */
    public String getHost() throws SQLException {
        String host =
                params.clusterName() == null
                        ? findClusterInPool(formatClusterFilterString())
                        : getClusterByName();
        return host + "-m";
    }

    /**
     * Supports finding & verifying the cluster indicated by name.
     *
     * @return the cluster of the given name
     * @throws InvalidURLException
     */
    public String getClusterByName() throws SQLException {
        try {
            Cluster cluster =
                    clusterControllerClient.getCluster(
                            params.projectId(), params.region(), params.clusterName());
            return cluster.getClusterName();
        } catch (ApiException e) {
            if (e.getStatusCode().getCode().equals(StatusCode.Code.NOT_FOUND)) {
                throw new InvalidURLException(
                        String.format(
                                "Unable to retrieve cluster information for %s/%s/%s.\n",
                                params.projectId(), params.region(), params.clusterName()),
                        e);
            }
            throw new SQLException(e);
        }
    }

    /**
     * Formats the user passed in clusterPoolLabel value to the filter format that Dataproc API
     * accpets: field = value [AND [field = value]] ...
     *
     * @return formatted filter string accepted by Dataproc Client API
     */
    public String formatClusterFilterString() throws InvalidURLException {
        Map<String, String> labels = new LinkedHashMap<>();
        // `ACTIVE` contains the `CREATING`, `UPDATING`, and `RUNNING` states
        labels.put("status.state", "ACTIVE");

        // clusterPoolLabel can be null, but filter is not empty string since
        // it at least has "status.state = ACTIVE"
        if (params.clusterPoolLabel() != null) {
            for (String label : params.clusterPoolLabel().split(":")) {
                checkUrl(
                        !label.isEmpty() && label.contains("="),
                        "'%s' Invalid clusterPoolLabel.\n"
                                + "Example format: clusterPoolLabel=field1=value1:field2=value2'",
                        label);
                String field = label.split("=")[0];
                String value = label.split("=")[1];

                if (field.equals("status.state")) {
                    throw new InvalidURLException(
                            "Please do not provide cluster status as label, "
                                    + "since DataprocDriver will use cluster with status.state = ACTIVE.");
                }

                // filed can be label.[key] or clusterName
                String labelName = field.equals("clusterName") ? field : "labels." + field;

                if (labels.containsKey(labelName)) {
                    throw new InvalidURLException(
                            String.format("%s. The key portion of a label must be unique.", labelName));
                }

                labels.put(labelName, value);
            }
        }

        String formattedFilter =
                labels.entrySet().stream()
                        .map(entry -> entry.getKey() + " = " + entry.getValue())
                        .collect(Collectors.joining(" AND "));
        return formattedFilter;
    }

    /**
     * Supports picking a cluster from cluster pool.
     *
     * @param filter formatted filter that matches the conditions client passed in
     * @return name of the suitable cluster with cluster pool
     * @throws IOException
     */
    public String findClusterInPool(String filter) throws SQLException {
        try {
            String clusterName = null;
            for (Cluster response :
                    clusterControllerClient
                            .listClusters(params.projectId(), params.region(), filter)
                            .iterateAll()) {
                // TODO: decide on how to pick a cluster among cluster pool
                clusterName = response.getClusterName();
            }
            return clusterName;
        } catch (ApiException e) {
            if (e.getStatusCode().getCode().equals(StatusCode.Code.NOT_FOUND)) {
                throw new InvalidURLException(
                        String.format(
                                "Unable to find active clusters matching label %s in %s/%s.\n",
                                filter, params.projectId(), params.region()),
                        e);
            }
            throw new SQLException(e);
        }
    }

    /** Allows class that initializes the ClusterControllerClient to close it. */
    public void closeClusterControllerClient() {
        this.clusterControllerClient.close();
    }
}