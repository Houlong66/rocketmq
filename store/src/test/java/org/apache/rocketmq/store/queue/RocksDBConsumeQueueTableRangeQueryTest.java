/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.queue;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.rocketmq.store.queue.RocksDBConsumeQueueTable.CQ_UNIT_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RocksDBConsumeQueueTable#rangeQuery} covering the seekAndReadNFlat optimization.
 */
public class RocksDBConsumeQueueTableRangeQueryTest {

    private ConsumeQueueRocksDBStorage rocksDBStorage;
    private DefaultMessageStore messageStore;
    private MessageStoreConfig storeConfig;
    private RocksDBConsumeQueueTable table;

    @Before
    public void setUp() {
        rocksDBStorage = mock(ConsumeQueueRocksDBStorage.class);
        messageStore = mock(DefaultMessageStore.class);
        storeConfig = new MessageStoreConfig();
        when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);
        table = new RocksDBConsumeQueueTable(rocksDBStorage, messageStore);
    }

    private byte[] buildCqValue(long phyOffset, int bodySize, long tagsCode, long storeTime) {
        ByteBuffer buf = ByteBuffer.allocate(CQ_UNIT_SIZE);
        buf.putLong(phyOffset);
        buf.putInt(bodySize);
        buf.putLong(tagsCode);
        buf.putLong(storeTime);
        return buf.array();
    }

    private byte[] buildFlatResult(int count) {
        byte[] flat = new byte[count * CQ_UNIT_SIZE];
        for (int i = 0; i < count; i++) {
            ByteBuffer buf = ByteBuffer.wrap(flat, i * CQ_UNIT_SIZE, CQ_UNIT_SIZE);
            buf.putLong((i + 1) * 1000L);
            buf.putInt(100 + i);
            buf.putLong(i);
            buf.putLong(System.currentTimeMillis());
        }
        return flat;
    }

    private void mockMultiGet(int entryCount) throws RocksDBException {
        ColumnFamilyHandle cfh = mock(ColumnFamilyHandle.class);
        when(rocksDBStorage.getDefaultCFHandle()).thenReturn(cfh);
        table.load();

        List<byte[]> multiGetResult = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            multiGetResult.add(buildCqValue(i * 500L, 50, i, System.currentTimeMillis()));
        }
        doReturn(multiGetResult).when(rocksDBStorage).multiGet(
            ArgumentMatchers.<List<ColumnFamilyHandle>>any(),
            ArgumentMatchers.<List<byte[]>>any());
    }

    // ==================== Config OFF → multiGet ====================

    @Test
    public void testRangeQuery_configOff_usesMultiGet() throws RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        storeConfig.setSeekAndReadNWhenRangeQueryRocksdbConsumeQueue(false);
        mockMultiGet(3);

        List<ByteBuffer> result = table.rangeQuery("topic", 0, 0, 3);
        assertEquals(3, result.size());

        verify(rocksDBStorage, never()).seekAndReadNCQ(any(), any(), anyInt(), anyInt());
    }

    // ==================== num=0 → empty ====================

    @Test
    public void testRangeQuery_numZero_returnsEmpty() throws RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        storeConfig.setSeekAndReadNWhenRangeQueryRocksdbConsumeQueue(true);

        List<ByteBuffer> result = table.rangeQuery("topic", 0, 0, 0);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testRangeQuery_numNegative_returnsEmpty() throws RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        storeConfig.setSeekAndReadNWhenRangeQueryRocksdbConsumeQueue(true);

        List<ByteBuffer> result = table.rangeQuery("topic", 0, 0, -1);
        assertTrue(result.isEmpty());
    }

    // ==================== num=1 → single get ====================

    @Test
    public void testRangeQuery_numOne_usesSingleGet() throws RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        storeConfig.setSeekAndReadNWhenRangeQueryRocksdbConsumeQueue(true);

        byte[] value = buildCqValue(5000L, 200, 42, System.currentTimeMillis());
        when(rocksDBStorage.getCQ(any())).thenReturn(value);

        List<ByteBuffer> result = table.rangeQuery("topic", 0, 10, 1);
        assertEquals(1, result.size());

        ByteBuffer bb = result.get(0);
        assertEquals(5000L, bb.getLong(0));
        assertEquals(200, bb.getInt(8));
        assertEquals(42L, bb.getLong(12));

        verify(rocksDBStorage, never()).seekAndReadNCQ(any(), any(), anyInt(), anyInt());
    }

    @Test
    public void testRangeQuery_numOne_keyNotFound_returnsEmpty() throws RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        storeConfig.setSeekAndReadNWhenRangeQueryRocksdbConsumeQueue(true);

        when(rocksDBStorage.getCQ(any())).thenReturn(null);

        List<ByteBuffer> result = table.rangeQuery("topic", 0, 999, 1);
        assertTrue(result.isEmpty());
    }

    // ==================== num>=2, all found → seek fast path ====================

    @Test
    public void testRangeQuery_seekFastPath_allFound() throws RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        storeConfig.setSeekAndReadNWhenRangeQueryRocksdbConsumeQueue(true);

        int count = 10;
        byte[] flat = buildFlatResult(count);
        when(rocksDBStorage.seekAndReadNCQ(any(), any(), anyInt(), anyInt())).thenReturn(flat);

        List<ByteBuffer> result = table.rangeQuery("topic", 0, 0, count);
        assertEquals(count, result.size());

        for (int i = 0; i < count; i++) {
            assertEquals((i + 1) * 1000L, result.get(i).getLong(0));
        }
    }

    @Test
    public void testRangeQuery_seekFastPath_twoEntries() throws RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        storeConfig.setSeekAndReadNWhenRangeQueryRocksdbConsumeQueue(true);

        byte[] flat = buildFlatResult(2);
        when(rocksDBStorage.seekAndReadNCQ(any(), any(), anyInt(), anyInt())).thenReturn(flat);

        List<ByteBuffer> result = table.rangeQuery("topic", 0, 100, 2);
        assertEquals(2, result.size());
        assertEquals(1000L, result.get(0).getLong(0));
        assertEquals(2000L, result.get(1).getLong(0));
    }

    // ==================== num>=2, partial → fallback to multiGet ====================

    @Test
    public void testRangeQuery_seekPartial_fallsBackToMultiGet() throws RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        storeConfig.setSeekAndReadNWhenRangeQueryRocksdbConsumeQueue(true);
        mockMultiGet(5);

        // seekAndReadNCQ returns only 3 out of 5 requested (has holes)
        byte[] flat = buildFlatResult(3);
        when(rocksDBStorage.seekAndReadNCQ(any(), any(), anyInt(), anyInt())).thenReturn(flat);

        List<ByteBuffer> result = table.rangeQuery("topic", 0, 0, 5);
        assertEquals(5, result.size());

        verify(rocksDBStorage).seekAndReadNCQ(any(), any(), anyInt(), anyInt());
    }

    @Test
    public void testRangeQuery_seekReturnsEmpty_fallsBackToMultiGet() throws RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        storeConfig.setSeekAndReadNWhenRangeQueryRocksdbConsumeQueue(true);
        mockMultiGet(3);

        when(rocksDBStorage.seekAndReadNCQ(any(), any(), anyInt(), anyInt())).thenReturn(new byte[0]);

        List<ByteBuffer> result = table.rangeQuery("topic", 0, 0, 3);
        assertEquals(3, result.size());
    }

    // ==================== Large batch ====================

    @Test
    public void testRangeQuery_seekFastPath_largeBatch() throws RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        storeConfig.setSeekAndReadNWhenRangeQueryRocksdbConsumeQueue(true);

        int count = 800;
        byte[] flat = buildFlatResult(count);
        when(rocksDBStorage.seekAndReadNCQ(any(), any(), anyInt(), anyInt())).thenReturn(flat);

        List<ByteBuffer> result = table.rangeQuery("topic", 0, 0, count);
        assertEquals(count, result.size());

        assertEquals(1000L, result.get(0).getLong(0));
        assertEquals(800 * 1000L, result.get(799).getLong(0));
    }

    // ==================== CQ value integrity ====================

    @Test
    public void testRangeQuery_seekPath_valueIntegrity() throws RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        storeConfig.setSeekAndReadNWhenRangeQueryRocksdbConsumeQueue(true);

        byte[] flat = new byte[3 * CQ_UNIT_SIZE];
        long[] phyOffsets = {100L, 200L, 300L};
        int[] bodySizes = {10, 20, 30};
        long[] tagsCodes = {111L, 222L, 333L};
        long[] storeTimes = {1000L, 2000L, 3000L};
        for (int i = 0; i < 3; i++) {
            ByteBuffer buf = ByteBuffer.wrap(flat, i * CQ_UNIT_SIZE, CQ_UNIT_SIZE);
            buf.putLong(phyOffsets[i]);
            buf.putInt(bodySizes[i]);
            buf.putLong(tagsCodes[i]);
            buf.putLong(storeTimes[i]);
        }
        when(rocksDBStorage.seekAndReadNCQ(any(), any(), anyInt(), anyInt())).thenReturn(flat);

        List<ByteBuffer> result = table.rangeQuery("topic", 0, 0, 3);
        assertEquals(3, result.size());

        for (int i = 0; i < 3; i++) {
            ByteBuffer bb = result.get(i);
            assertEquals(phyOffsets[i], bb.getLong(0));
            assertEquals(bodySizes[i], bb.getInt(8));
            assertEquals(tagsCodes[i], bb.getLong(12));
            assertEquals(storeTimes[i], bb.getLong(20));
        }
    }

    // ==================== Default config ====================

    @Test
    public void testRangeQuery_defaultConfig_seekEnabled() throws RocksDBException {
        if (MixAll.isMac()) {
            return;
        }
        assertTrue(storeConfig.isSeekAndReadNWhenRangeQueryRocksdbConsumeQueue());

        byte[] flat = buildFlatResult(5);
        when(rocksDBStorage.seekAndReadNCQ(any(), any(), anyInt(), anyInt())).thenReturn(flat);

        List<ByteBuffer> result = table.rangeQuery("topic", 0, 0, 5);
        assertEquals(5, result.size());

        verify(rocksDBStorage).seekAndReadNCQ(any(), any(), anyInt(), anyInt());
    }
}
