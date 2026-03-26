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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.smoke;

import com.google.common.collect.ImmutableList;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.StoreType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;

/**
 * Integration tests for seekAndReadNFlat optimization in RocksDB ConsumeQueue.
 * Verifies that the seekAndReadNFlat path produces correct results for
 * single message (N=1 get path), batch reads (N>=2 seekAndReadNFlat path),
 * and tag filtering scenarios.
 */
public class RocksDBCQSeekAndReadNIT extends BaseConf {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBCQSeekAndReadNIT.class);

    private RMQNormalProducer producer = null;
    private String topic = null;

    @Before
    public void setUp() throws Exception {
        resetBrokersToRocksDB();
        topic = initTopic();
        logger.info("use topic: {}", topic);
        producer = getProducer(NAMESRV_ADDR, topic);
    }

    @After
    public void tearDown() {
        BaseConf.shutdown();
    }

    /**
     * Test N=1 path: single message send and receive via get().
     */
    @Test
    public void testSendAndReceive_SingleMessage() {
        String group = initConsumerGroup();
        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, group, topic, "*", new RMQNormalListener());

        int msgSize = 1;
        producer.send(msgSize);

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }

    /**
     * Test N>=2 path with tag filter: send mixed tags, consume only TagA.
     * Core test for seekAndReadNFlat correctness + tag filtering.
     */
    @Test
    public void testSendAndReceiveWithTagFilter_Batch() {
        String group = initConsumerGroup();
        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, group, topic, "TagA", new RMQNormalListener());

        int msgSizePerTag = 50;
        producer.send("TagA", msgSizePerTag);
        producer.send("TagB", msgSizePerTag);

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);
        assertThat(consumer.getListener().getAllMsgBody().size()).isEqualTo(msgSizePerTag);
    }

    /**
     * Test N>=2 path with wildcard subscription: all messages received.
     */
    @Test
    public void testSendAndReceive_AllTags() {
        String group = initConsumerGroup();
        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, group, topic, "*", new RMQNormalListener());

        int msgSizePerTag = 50;
        producer.send("TagA", msgSizePerTag);
        producer.send("TagB", msgSizePerTag);

        int totalMsgSize = msgSizePerTag * 2;
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }

    /**
     * Test rangeQuery works correctly across multiple queues.
     */
    @Test
    public void testSendAndReceive_MultipleQueues() throws Exception {
        String group = initConsumerGroup();
        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, group, topic, "*", new RMQNormalListener());

        List<MessageQueue> messageQueues = producer.getProducer().fetchPublishMessageQueues(topic);
        int msgSizePerQueue = 10;
        for (MessageQueue mq : messageQueues) {
            producer.send(msgSizePerQueue, mq);
        }

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }

    private void resetBrokersToRocksDB() {
        {
            brokerController1.shutdown();
            MessageStoreConfig storeConfig = brokerController1.getMessageStoreConfig();
            BrokerConfig brokerConfig = brokerController1.getBrokerConfig();
            storeConfig.setStoreType(StoreType.DEFAULT_ROCKSDB.getStoreType());
            storeConfig.setSeekAndReadNWhenRangeQueryRocksdbConsumeQueue(true);
            brokerController1 = IntegrationTestBase.createAndStartBroker(storeConfig, brokerConfig);
        }
        {
            brokerController2.shutdown();
            MessageStoreConfig storeConfig = brokerController2.getMessageStoreConfig();
            BrokerConfig brokerConfig = brokerController2.getBrokerConfig();
            storeConfig.setStoreType(StoreType.DEFAULT_ROCKSDB.getStoreType());
            storeConfig.setSeekAndReadNWhenRangeQueryRocksdbConsumeQueue(true);
            brokerController2 = IntegrationTestBase.createAndStartBroker(storeConfig, brokerConfig);
        }
        {
            brokerController3.shutdown();
            MessageStoreConfig storeConfig = brokerController3.getMessageStoreConfig();
            BrokerConfig brokerConfig = brokerController3.getBrokerConfig();
            storeConfig.setStoreType(StoreType.DEFAULT_ROCKSDB.getStoreType());
            storeConfig.setSeekAndReadNWhenRangeQueryRocksdbConsumeQueue(true);
            brokerController3 = IntegrationTestBase.createAndStartBroker(storeConfig, brokerConfig);
        }
        brokerControllerList = ImmutableList.of(brokerController1, brokerController2, brokerController3);
        brokerControllerMap = brokerControllerList.stream().collect(
            Collectors.toMap(input -> input.getBrokerConfig().getBrokerName(), Function.identity()));
    }
}
