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

import org.junit.Before;
import org.junit.Test;

public class DataprocDriverTest {
    private DataprocDriver driver;

    @Before
    public void setUp() {
        driver = new DataprocDriver();
    }

    @Test
    public void acceptsURL_null() {
        String url = null;
        assertThat(driver.acceptsURL(url)).isFalse();
    }

    @Test
    public void acceptsURL_Hive_empty() {
        String url = "";
        assertThat(driver.acceptsURL(url)).isFalse();
    }

    @Test
    public void acceptsURL_Hive_incomplete() {
        String url = "jdbc:dataproc://";
        assertThat(driver.acceptsURL(url)).isFalse();
    }

    @Test
    public void acceptsURL_Hive_incomplete_isAccepted() {
        // This is accepted as in later method a more specific error exception will be thrown
        String url = "jdbc:dataproc://hive/";
        assertThat(driver.acceptsURL(url)).isTrue();
    }

    @Test
    public void acceptsURL_Hive_correct() {
        String url = "jdbc:dataproc://hive/;clusterName=test";
        assertThat(driver.acceptsURL(url)).isTrue();
    }

    @Test
    public void acceptsURL_Hive_otherProtocol_notAccepted() {
        String url = "jdbc:dataproc://other-protocol/";
        assertThat(driver.acceptsURL(url)).isFalse();
    }

    @Test
    public void acceptsURL_Hive_incorrectScheme() {
        String url = "jdbc:data://hive/;clusterName=test";
        assertThat(driver.acceptsURL(url)).isFalse();
    }
}