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

import static com.google.cloud.dataproc.jdbc.HiveUrlUtils.HIVE_DEFAULT_DATABASE;
import static com.google.cloud.dataproc.jdbc.HiveUrlUtils.HIVE_DEFAULT_PORT;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/** Helper class to extract Hive connection parameters */
@AutoValue
abstract class HiveJdbcConnectionOptions {

    private static final String TRANSPORT_MODE = "http";
    private static final String HTTP_PATH = "cliservice";

    // These client side params are required by DataprocDriver
    abstract String projectId();

    abstract String region();

    abstract String httpPath();

    abstract String transportMode();

    abstract int port();

    abstract String dbName();

    @Nullable
    abstract String clusterName();

    @Nullable
    abstract String clusterPoolLabel();

    //  Do not parse these other parameters and pass them directly to HiveConnection
    @Nullable
    abstract String otherSessionConfs();

    @Nullable
    abstract String hiveConfs();

    @Nullable
    abstract String hiveVars();

    static HiveJdbcConnectionOptions.Builder builder() {
        // Set default values
        return new AutoValue_HiveJdbcConnectionOptions.Builder()
                .setHttpPath(HTTP_PATH)
                .setTransportMode(TRANSPORT_MODE)
                .setPort(HIVE_DEFAULT_PORT)
                .setDbName(HIVE_DEFAULT_DATABASE)
                .setOtherSessionConfs(null)
                .setHiveVars(null)
                .setHiveConfs(null);
    }

    @AutoValue.Builder
    abstract static class Builder {
        abstract HiveJdbcConnectionOptions.Builder setPort(int value);

        abstract HiveJdbcConnectionOptions.Builder setDbName(String value);

        abstract HiveJdbcConnectionOptions.Builder setHttpPath(String value);

        abstract HiveJdbcConnectionOptions.Builder setTransportMode(String value);

        abstract HiveJdbcConnectionOptions.Builder setOtherSessionConfs(String value);

        abstract HiveJdbcConnectionOptions.Builder setHiveConfs(String value);

        abstract HiveJdbcConnectionOptions.Builder setHiveVars(String value);

        abstract HiveJdbcConnectionOptions.Builder setClusterName(String value);

        abstract HiveJdbcConnectionOptions.Builder setClusterPoolLabel(String value);

        abstract HiveJdbcConnectionOptions.Builder setProjectId(String value);

        abstract HiveJdbcConnectionOptions.Builder setRegion(String value);

        abstract HiveJdbcConnectionOptions build();
    }
}