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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.util.Date;
import org.apache.hive.jdbc.shaded.org.apache.hive.org.apache.http.Header;
import org.apache.hive.jdbc.shaded.org.apache.hive.org.apache.http.HttpException;
import org.apache.hive.jdbc.shaded.org.apache.hive.org.apache.http.HttpRequest;
import org.apache.hive.jdbc.shaded.org.apache.hive.org.apache.http.client.methods.HttpGet;
import org.apache.hive.jdbc.shaded.org.apache.hive.org.apache.http.protocol.HttpContext;
import org.junit.Before;
import org.junit.Test;

public class DataprocCGAuthInterceptorTest {
    private static final String TOKEN_VALUE_1 = "application-default-access-token1";
    private static final String TOKEN_VALUE_2 = "application-default-access-token2";
    private static final long HOUR = 3600 * 1000;

    // Mock GoogleCredentials
    private static GoogleCredentials mockCredentials;

    private DataprocCGAuthInterceptor interceptor;
    private HttpRequest request;
    private HttpContext context;

    @Before
    public void setUp() {
        mockCredentials = mock(GoogleCredentials.class);

        when(mockCredentials.getAccessToken())
                .thenReturn(new AccessToken(TOKEN_VALUE_1, new Date(System.currentTimeMillis() + HOUR)))
                .thenReturn(new AccessToken(TOKEN_VALUE_2, new Date(System.currentTimeMillis() + HOUR)));

        request = new HttpGet();
        context =
                new HttpContext() {
                    @Override
                    public Object getAttribute(String s) {
                        return null;
                    }

                    @Override
                    public void setAttribute(String s, Object o) {}

                    @Override
                    public Object removeAttribute(String s) {
                        return null;
                    }
                };
    }

    @Test
    public void testConstructor() throws IOException {
        interceptor = new DataprocCGAuthInterceptor(mockCredentials);
        verify(mockCredentials, times(1)).refresh();
    }

    @Test
    public void testProcess() throws IOException, HttpException {
        interceptor = new DataprocCGAuthInterceptor(mockCredentials);
        interceptor.process(request, context);

        Header[] headers = request.getHeaders("Proxy-Authorization");
        assertThat(headers.length).isEqualTo(1);
        assertThat(headers[0].getName()).isEqualTo("Proxy-Authorization");
        assertThat(headers[0].getValue()).isEqualTo("Bearer " + TOKEN_VALUE_1);
    }

    @Test
    public void testProcess_newToken() throws IOException, HttpException {
        interceptor = new DataprocCGAuthInterceptor(mockCredentials);
        interceptor.process(request, context);

        Header[] headers = request.getHeaders("Proxy-Authorization");
        assertThat(headers.length).isEqualTo(1);
        assertThat(headers[0].getName()).isEqualTo("Proxy-Authorization");
        assertThat(headers[0].getValue()).isEqualTo("Bearer " + TOKEN_VALUE_1);

        interceptor.process(request, context);

        headers = request.getHeaders("Proxy-Authorization");
        assertThat(headers.length).isEqualTo(1); // Verify that only one header with CG_AUTH_HEADER
        assertThat(headers[0].getName()).isEqualTo("Proxy-Authorization");
        assertThat(headers[0].getValue()).isEqualTo("Bearer " + TOKEN_VALUE_2);
    }
}