/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.storage.adaptive;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteFlags;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.*;
import java.util.*;

import static org.junit.Assert.assertTrue;


public class TestAdaptiveMapTranHistoryList {

    private final String HOST = "172.28.128.5";
    private final String NAMESPACE = "test";
    private final String SET = "testAdapt";
    private final String MAP_BIN = "mapBin";
    private final int MAP_SPLIT_SIZE = 100;
    private AdaptiveMap library;
    private AerospikeClient client;
    private IAdaptiveMap adaptiveMapWithValueKey;
    private IAdaptiveMap adaptiveMapWithDigestKey;

    @Before
    public void setUp() throws Exception {
        client = new AerospikeClient(HOST, 3000);
        MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);

        // Use the underlying functions for restricted functions
        library = new AdaptiveMap(client, NAMESPACE, SET, MAP_BIN, policy, false, MAP_SPLIT_SIZE);

        // Set up the interface to use for primary operations
        adaptiveMapWithValueKey = library;

        adaptiveMapWithDigestKey = new AdaptiveMap(client, NAMESPACE, SET, MAP_BIN, policy, true, MAP_SPLIT_SIZE);
    }

    @After
    public void tearDown() throws Exception {
        client.close();
    }

    @Test
    public void testMultiRecordGetTranHistoryList() {
        System.out.printf("\n*** testMultiRecordGet ***\n");
        // Clean up after previous runs
        client.truncate(null, NAMESPACE, SET, null);
        final int COUNT_PER_DAY = 10 * MAP_SPLIT_SIZE;
        String basePart = "12345";
        LocalDateTime start = LocalDateTime.of(LocalDate.of(2020, 03, 01), LocalTime.of(0, 0, 0));
        LocalDateTime end = LocalDateTime.of(LocalDate.of(2020, 03, 30), LocalTime.of(0, 0, 0));
        String trnHistoryJson = "{ \"tranId\": \"transactionId\", \"tranDate\": \"1583001761000\", \"trnType\": \"C\", \"tranAmount\": 2002.4, \"partTrnSerialNo\": \"01\" }";
        long now = System.nanoTime();
        for (LocalDateTime date = start; date.isBefore(end); date = date.plusDays(1)) {
            String baseKey = basePart + ":" + date.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            for (int i = 0; i < 10000; i++) {
                Instant instant = date.atZone(ZoneId.systemDefault()).plusSeconds(i).toInstant();
                try {
                    List<String> response = (List<String>) this.adaptiveMapWithValueKey.get(baseKey, instant.toEpochMilli());
                    response.add(trnHistoryJson.replace("transactionId", UUID.randomUUID().toString()));
                    this.adaptiveMapWithValueKey.put(baseKey, instant.toEpochMilli(), null, Value.get(response));
                } catch(Exception e) {
                    List<String> transactions = Arrays.asList(trnHistoryJson.replace("transactionId", UUID.randomUUID().toString()));
                    this.adaptiveMapWithValueKey.put(baseKey, instant.toEpochMilli(), null, Value.get(transactions));
                }
            }
            System.out.println(baseKey);
        }
        long time = System.nanoTime() - now;
        System.out.printf("Saved %d days records with %d total entries in %.1fms\n", 30, 30000, time / 1000000.0);
    }

    @Test
    public void testGetDayRecordList() {
        try {
            List<String> response = (List<String>) this.adaptiveMapWithValueKey.get("12345:15830010000000", 1583001000000l);
            System.out.println(response.size());
        } catch(Exception e) {
            e.printStackTrace();
        }

        long now = System.nanoTime();
        String[] keys = new String[]{"12345:1583001000000",
                "12345:1583087400000",
                "12345:1583173800000",
                "12345:1583260200000",
                "12345:1583346600000",
                "12345:1583433000000",
                "12345:1583519400000",
                "12345:1583605800000",
                "12345:1583692200000",
                "12345:1583778600000",
                "12345:1583865000000",
                "12345:1583951400000",
                "12345:1584037800000",
                "12345:1584124200000",
                "12345:1584210600000",
                "12345:1584297000000",
                "12345:1584383400000",
                "12345:1584469800000",
                "12345:1584556200000",
                "12345:1584642600000",
                "12345:1584729000000",
                "12345:1584815400000",
                "12345:1584901800000",
                "12345:1584988200000",
                "12345:1585074600000",
                "12345:1585161000000",
                "12345:1585247400000",
                "12345:1585333800000",
                "12345:1585420200000"};
//        TreeMap<Object, Object>[] treeMap = this.adaptiveMapWithValueKey.getAll(null, keys);
//        long time = System.nanoTime() - now;
//        System.out.printf("Retrieved %d days records with %d total entries in %.1fms\n", 30, 30000, time / 1000000.0);
//        for(int i=0; i< 29; i++) {
//            System.out.println(treeMap[i].size());
//        }
//        System.out.println(treeMap[25]);
    }
}
