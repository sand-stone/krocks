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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kdb.Client;
import kdb.KdbException;

public class KDBContext {
  private static final Logger LOG = LoggerFactory.getLogger(KDBContext.class);
  List<Receiver> receivers = new ArrayList<Receiver>();
  private Client client;
  private Properties _props;

  public KDBContext(Properties props) {
    _props = props;
    initKDB();
  }

  private void initKDB() {
    String endpoint = _props.getProperty("kdb.endpoint");
    String dbname = _props.getProperty("kdb.dbname");
    try {
      client = new Client(endpoint, dbname);
    } catch (KdbException ex) {
      LOG.info(ex.toString());
    }
  }

  public Client getKdb() {
    return client;
  }

  public void upsertData(Object key, Object value) {

  }

  public void receiverStream(Receiver receiver) {
    receivers.add(receiver);
  }

  public long batchDuration() {
    return -1;
  }

  public void batchComplete(KafkaConfig kafkaConfig, PIDController controller) {
    final int DEFAULT_RATE = kafkaConfig._fetchSizeBytes;
    final int MIN_RATE = kafkaConfig._minFetchSizeBytes;
    final int fillFreqMs = kafkaConfig._fillFreqMs;
    final KafkaConfig config = kafkaConfig;
    long processingDelay = -1;
    long schedulingDelay = -1;
    long batchDuration = -1;
    int partitionCount = -1;
    int batchFetchSize = DEFAULT_RATE;
    ZkState state = new ZkState((String) config._stateConf.get(Config.ZOOKEEPER_CONSUMER_CONNECTION));

    int newRate = controller.calculateRate(
                                           System.currentTimeMillis(),
                                           batchDuration,
                                           partitionCount,
                                           batchFetchSize,
                                           fillFreqMs,
                                           schedulingDelay,
                                           processingDelay);

    // Setting to Min Rate
    if (newRate <= 0) {
      newRate = MIN_RATE;
    }

    String path = ratePath(config);
    Map<Object, Object> metadata = (Map<Object, Object>) ImmutableMap.builder()
      .put("consumer",ImmutableMap.of("id", config._stateConf.get("kafka.consumer.id")))
      .put("topic", config._stateConf.get("kafka.topic"))
      .put("rate", newRate)
      .build();
    state.writeJSON(path, metadata);
    state.close();
  }

  private String ratePath(KafkaConfig config) {
    return config._stateConf.get(Config.ZOOKEEPER_CONSUMER_PATH)
      + "/" + config._stateConf.get(Config.KAFKA_CONSUMER_ID)
      + "/" + config._stateConf.get(Config.KAFKA_TOPIC) + "/newrate";
  }

  public void start() {
    for(Receiver receiver : receivers) {
      receiver.start();
    }
  }

  public void awaitTermination() {
    while(true) {
      try {
        Thread.currentThread().sleep(1000);
      } catch (InterruptedException ex) {}
    }
  }

}
