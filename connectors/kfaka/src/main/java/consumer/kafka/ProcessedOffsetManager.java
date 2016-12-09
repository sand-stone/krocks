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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

public class ProcessedOffsetManager {

  public static Map<Integer, Iterable<Long>> getPartitionOffset(KDBContext ctx) {
    Map<Integer, Iterable<Long>> partitonOffset = new HashMap<Integer, Iterable<Long>>();
    return partitonOffset;
  }

  public static void persists(Map<Integer, Iterable<Long>> partitonOffset, Properties props) {
    Map<Integer, Long> partitionOffsetMap = new HashMap<Integer, Long>();
    for(Map.Entry<Integer, Iterable<Long>> entry : partitonOffset.entrySet()) {
      int partition = entry.getKey();
      Long offset = getMaximum(entry.getValue());
      partitionOffsetMap.put(partition, offset);
      persistProcessedOffsets(props, partitionOffsetMap);
    }
  }


  private static <T extends Comparable<T>> T getMaximum(Iterable<T> values) {
    T max = null;
    for (T value : values) {
      if (max == null || max.compareTo(value) < 0) {
        max = value;
      }
    }
    return max;
  }

  private static void persistProcessedOffsets(Properties props, Map<Integer, Long> partitionOffsetMap) {
    ZkState state = new ZkState(props.getProperty(Config.ZOOKEEPER_CONSUMER_CONNECTION));
    for(Map.Entry<Integer, Long> po : partitionOffsetMap.entrySet()) {
      Map<Object, Object> data = (Map<Object, Object>) ImmutableMap
        .builder()
        .put("consumer",ImmutableMap.of("id",props.getProperty(Config.KAFKA_CONSUMER_ID)))
        .put("offset", po.getValue())
        .put("partition",po.getKey())
        .put("broker",ImmutableMap.of("host", "", "port", ""))
        .put("topic", props.getProperty(Config.KAFKA_TOPIC)).build();
      String path = processedPath(po.getKey(), props);
      try{
        state.writeJSON(path, data);
      }catch (Exception ex) {
        state.close();
        throw ex;
      }
    }
    state.close();
  }

  private static String processedPath(int partition, Properties props) {
    return props.getProperty(Config.ZOOKEEPER_CONSUMER_PATH)
      + "/" + props.getProperty(Config.KAFKA_CONSUMER_ID) + "/"
      + props.getProperty(Config.KAFKA_TOPIC)
      + "/processed/" + "partition_"+ partition;
  }

}
