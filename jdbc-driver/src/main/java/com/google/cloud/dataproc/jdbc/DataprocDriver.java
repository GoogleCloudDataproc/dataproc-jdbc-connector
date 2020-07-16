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

import static com.google.cloud.dataproc.jdbc.HiveUrlUtils.formatHiveUrl;

import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/** DataprocDriver class to create connection with Hive. */
public class DataprocDriver implements Driver {

    // Expected JDBC URL prefix format
    public static final String DATAPROC_JDBC_HIVE_URL_SCHEMA = "jdbc:dataproc://hive/";

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return acceptsURL(url) ? createConnection(url, info) : null;
    }

    @Override
    public boolean acceptsURL(String url) {
        // Other protocols may be supported later, but for now only accept Hive
        return url != null && url.startsWith(DATAPROC_JDBC_HIVE_URL_SCHEMA);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    /**
     * Establishes the connection using translated URL. Translated URL format:
     * jdbc:hive2://<host>:<port>/<dbName>;transportMode=http;httpPath=<http_endpoint>;
     * <otherSessionConfs>?<hiveConfs>#<hiveVars>
     *
     * @param url client passed in JDBC URL
     * @param info client passed in connection properties
     * @return the created Hive Connection
     */
    @VisibleForTesting
    Connection createConnection(String url, Properties info) throws SQLException {
        // Valid url format:
        // jdbc:dataproc://<protocol>/<db>;clusterName=<>;other_sess_var_list?hive_conf_list#hive_var_list

        if (url.startsWith(DATAPROC_JDBC_HIVE_URL_SCHEMA)) {
            try {
                String hiveURL = formatHiveUrl(url);
            } catch (InvalidURLException e) {
                throw new SQLException(e.getCause());
            }
            // TODO: return HiveConnection
            return null;
        } else {
            // TODO: support other protocol
            return null;
        }
    }
}