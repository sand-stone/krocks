/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package consumer.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import consumer.kafka.client.KafkaRangeReceiver;
import consumer.kafka.client.KafkaReceiver;

@SuppressWarnings("serial")
public class ReceiverLauncher implements Serializable {

  public static final Logger LOG = LoggerFactory
    .getLogger(ReceiverLauncher.class);
  private static String _zkPath;

  public static void launch(
                            KDBContext ctx,
                            Properties pros,
                            int numberOfReceivers) {
    createStream(ctx, pros, numberOfReceivers);
  }

  private static void createStream(
                                   KDBContext ctx,
                                   Properties pros,
                                   int numberOfReceivers) {
    int numberOfPartition;
    KafkaConfig kafkaConfig = new KafkaConfig(pros);
    ZkState zkState = new ZkState(kafkaConfig);
    String numberOfPartitionStr = (String) pros.getProperty(Config.KAFKA_PARTITIONS_NUMBER);
    if (numberOfPartitionStr != null) {
      numberOfPartition = Integer.parseInt(numberOfPartitionStr);
    } else {
      _zkPath = (String) kafkaConfig._stateConf.get(Config.ZOOKEEPER_BROKER_PATH);
      String _topic = (String) kafkaConfig._stateConf.get(Config.KAFKA_TOPIC);
      numberOfPartition = getNumPartitions(zkState, _topic);
    }

    // Create as many Receiver as Partition
    if (numberOfReceivers >= numberOfPartition) {
      for (int i = 0; i < numberOfPartition; i++) {
        ctx.receiverStream(new KafkaReceiver(ctx, pros, i));
      }
    } else {
      // create Range Receivers..
      Map<Integer, Set<Integer>> rMap = new HashMap<Integer, Set<Integer>>();
      for (int i = 0; i < numberOfPartition; i++) {
        int j = i % numberOfReceivers;
        Set<Integer> pSet = rMap.get(j);
        if (pSet == null) {
          pSet = new HashSet<Integer>();
          pSet.add(i);
        } else {
          pSet.add(i);
        }
        rMap.put(j, pSet);
      }
      for (int i = 0; i < numberOfReceivers; i++) {
        ctx.receiverStream(new KafkaRangeReceiver(ctx, pros, rMap.get(i)));
      }
    }

    boolean backPressureEnabled = (boolean) kafkaConfig._backpressureEnabled;
    if (backPressureEnabled) {
      initializeLisnter(ctx, kafkaConfig, numberOfPartition);
    }
  }

  private static void initializeLisnter(
                                        KDBContext ctx,
                                        KafkaConfig kafkaConfig,
                                        int numberOfPartition) {

    final int DEFAULT_RATE = kafkaConfig._fetchSizeBytes;
    final int MIN_RATE = kafkaConfig._minFetchSizeBytes;
    final int fillFreqMs = kafkaConfig._fillFreqMs;
    final KafkaConfig config = kafkaConfig;
    final int partitionCount = numberOfPartition;
    final PIDController controller =
      new PIDController(
                        kafkaConfig._proportional,
                        kafkaConfig._integral,
                        kafkaConfig._derivative);

  }

  private static int getNumPartitions(ZkState zkState, String topic) {
    try {
      String topicBrokersPath = partitionPath(topic);
      List<String> children = zkState.getCurator().getChildren().forPath(topicBrokersPath);
      return children.size();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String partitionPath(String topic) {
    return _zkPath + "/topics/" + topic + "/partitions";
  }
}
