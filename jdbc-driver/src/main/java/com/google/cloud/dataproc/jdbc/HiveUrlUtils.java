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

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Helper methods to establish Connection for DataprocDriver. */
public class HiveUrlUtils {
    public static final String HIVE_DEFAULT_DATABASE = "";
    public static final int HIVE_DEFAULT_PORT = 443;
    public static final String URL_JDBC_PREFIX = "jdbc:";

    private static Set<String> NON_SESSION_CONFS =
            ImmutableSet.of(
                    "projectId",
                    "region",
                    "clusterPoolLabel",
                    "clusterName",
                    "port",
                    "httpPath",
                    "transportMode");

    /**
     * Parses client url and extracts Hive connection parameters.
     *
     * @param url verified url passed in by client, already of valid format possible formats:
     *     jdbc:dataproc://hive/<dbName>;clusterName=<>;projectId=<>;region=<>;port=<>
     *     jdbc:dataproc://hive/<dbName>;clusterName=<>;projectId=<>;region=<>;clusterPoolLabel=foo=bar:env=staging;
     * @return HiveJdbcConnectionParam object with fields correctly set
     * @throws IOException
     * @throws InvalidURLException
     */
    public static HiveJdbcConnectionOptions parseHiveUrl(String url) throws InvalidURLException {
        HiveJdbcConnectionOptions.Builder paramBuilder = HiveJdbcConnectionOptions.builder();

        // Stripped out for the purpose of using URI
        String urlStripped = url.substring(URL_JDBC_PREFIX.length());
        URI jdbcURI = URI.create(urlStripped);

        // getPath() will return: /<dbName>;<sessionConfs>, substring gets rid of the "/"
        String dbAndSessionConfs = jdbcURI.getPath().substring(1);
        // getQuery() will return the info after '?'
        String hiveConfs = jdbcURI.getQuery();
        // getFragmeng() will return the info after '#'
        String hiveVars = jdbcURI.getFragment();

        String[] params = dbAndSessionConfs.split(";");

        // Incorrect database name provided
        checkUrl(
                params.length == 0 || !params[0].contains("="),
                "'%s' Database name must not contain '='. "
                        + "If no dbName is specified, format should be: %s",
                params.length != 0 ? params[0] : "",
                "jdbc:dataproc://hive/;clusterName=<clusterName>");

        paramBuilder.setDbName(params.length == 0 ? HIVE_DEFAULT_DATABASE : params[0]);
        // Params that do not need parsing
        paramBuilder.setHiveConfs(hiveConfs);
        paramBuilder.setHiveVars(hiveVars);

        Map<String, String> paramsMap = paramsToMap(params, /* startIndex= */ 1);

        // Get otherSessionConfigs
        String otherSessionConfs =
                paramsMap.entrySet().stream()
                        .filter(entry -> !NON_SESSION_CONFS.contains(entry.getKey()))
                        .map(Object::toString)
                        .collect(Collectors.joining(";"));
        if (!otherSessionConfs.isEmpty()) {
            paramBuilder.setOtherSessionConfs(otherSessionConfs);
        }

        // Client shouldn't pass in the session variables that are already set
        checkUrl(
                !paramsMap.containsKey("httpPath"), "Invalid variable.\nPlease do not include httpPath.");
        checkUrl(
                !paramsMap.containsKey("transportMode"),
                "Invalid variable.\nPlease do not include transportMode.");
        checkUrl(
                !paramsMap.containsKey("port"),
                "'port=%s'\nPlease indicate correct port number or remove the field.",
                paramsMap.get("port"));

        // Client must specify projectId and region
        checkUrl(paramsMap.containsKey("projectId"), "Please provide projectId.");
        checkUrl(paramsMap.containsKey("region"), "Please provide region.");
        paramBuilder
                .setRegion(paramsMap.get("region"))
                .setProjectId(paramsMap.get("projectId"))
                .setClusterName(paramsMap.get("clusterName"))
                .setClusterPoolLabel(paramsMap.get("clusterPoolLabel"));

        return paramBuilder.build();
    }

    /**
     * Helper method to check condition and throw InvalidURLException
     *
     * @param condition the condition to meet
     * @param msgFmt the error message
     * @param msgParams optional message parameter
     * @throws InvalidURLException
     */
    public static void checkUrl(boolean condition, String msgFmt, String... msgParams)
            throws InvalidURLException {
        if (!condition) {
            throw new InvalidURLException(String.format(msgFmt, (Object[]) msgParams));
        }
    }

    /**
     * Helper method that turns the field=value pair into map.
     *
     * @param params parameters given by the client
     * @param startIndex start of sessionConfs
     * @throws InvalidURLException
     */
    private static Map<String, String> paramsToMap(String[] params, int startIndex)
            throws InvalidURLException {
        Map<String, String> paramsMap = new LinkedHashMap<>();
        for (int i = startIndex; i < params.length; i++) {
            String paramPair = params[i];

            checkUrl(
                    !paramPair.isEmpty() && paramPair.indexOf("=") != -1,
                    "'%s' Please provide correct format of session configs:\n" + "key=value",
                    paramPair);

            // Cannot simply split by "=" because for clusterPoolLabel that value could contain "=" as
            // well
            String field = paramPair.substring(0, paramPair.indexOf("="));
            String value = paramPair.substring(paramPair.indexOf("=") + 1);
            paramsMap.put(field, value);
        }
        return paramsMap;
    }
}