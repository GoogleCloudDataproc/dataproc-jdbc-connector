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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import org.apache.hive.jdbc.shaded.org.apache.hive.org.apache.http.HttpException;
import org.apache.hive.jdbc.shaded.org.apache.hive.org.apache.http.HttpRequest;
import org.apache.hive.jdbc.shaded.org.apache.hive.org.apache.http.HttpRequestInterceptor;
import org.apache.hive.jdbc.shaded.org.apache.hive.org.apache.http.protocol.HttpContext;

/** HttpRequestInterceptor used to add header for Dataproc Component Gateway Authorization. */
public class DataprocCGAuthInterceptor implements HttpRequestInterceptor {

    public static final String CG_AUTH_HEADER = "Proxy-Authorization";
    public static final String BEARER_PREFIX = "Bearer ";

    private GoogleCredentials credentials;

    // Default constructor used by HiveConnection
    public DataprocCGAuthInterceptor() throws IOException {
        this.credentials = GoogleCredentials.getApplicationDefault();
        this.credentials = credentials.createScoped("https://www.googleapis.com/auth/cloud-platform");
        credentials.refresh();
    }

    // Constructor dependency injection for testing
    public DataprocCGAuthInterceptor(GoogleCredentials credentials) throws IOException {
        this.credentials = credentials;
        credentials.refresh();
    }

    @Override
    public void process(HttpRequest httpRequest, HttpContext httpContext)
            throws HttpException, IOException {
        if (httpRequest.containsHeader(CG_AUTH_HEADER)) {
            httpRequest.removeHeaders(CG_AUTH_HEADER);
        }
        httpRequest.addHeader(CG_AUTH_HEADER, getAccessToken());
    }

    /**
     * Gets the application default access token from client environment.
     *
     * @return the refreshed Bearer token
     */
    public String getAccessToken() throws IOException {
        credentials.refreshIfExpired();
        AccessToken accessToken = credentials.getAccessToken();
        return BEARER_PREFIX + accessToken.getTokenValue();
    }
}