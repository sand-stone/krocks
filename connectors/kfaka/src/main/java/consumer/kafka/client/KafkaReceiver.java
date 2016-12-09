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

package consumer.kafka.client;

import java.util.Properties;
import java.util.List;
import java.util.UUID;
import consumer.kafka.KafkaConfig;
import consumer.kafka.KafkaConsumer;
import consumer.kafka.MessageAndMetadata;
import consumer.kafka.Receiver;
import consumer.kafka.ZkState;
import consumer.kafka.KDBContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;

@SuppressWarnings("serial")
public class KafkaReceiver extends Receiver {
  public static final Logger LOG = LoggerFactory.getLogger(KafkaReceiver.class);
  private int _partitionId;

  public KafkaReceiver(KDBContext ctx, Properties props, int partitionId) {
    super(ctx);
    this._props = props;
    _partitionId = partitionId;
  }

  public void onStart() {
    start();
  }

  public void restart(String msg, Throwable ex, int delay) {

  }

  public boolean isStopped() {
    return false;
  }

  public void start() {
    LOG.info("Kafka receiver start");
    // Start the thread that receives data over a connection
    KafkaConfig kafkaConfig = new KafkaConfig(_props);
    ZkState zkState = new ZkState(kafkaConfig);
    _kConsumer = new KafkaConsumer(kafkaConfig, zkState, this);
    _kConsumer.open(_partitionId);
    Thread.UncaughtExceptionHandler eh = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread th, Throwable ex) {
          if (ex instanceof InterruptedException) {
            th.interrupt();
            stop(" Stopping Receiver for partition "
                 + _partitionId
                 + " due to "
                 + ex);
          } else {
            restart("Restarting Receiver for Partition " + _partitionId, ex, 5000);
          }
        }
      };
    _consumerThread = new Thread(_kConsumer);
    _consumerThread.setDaemon(true);
    _consumerThread.setUncaughtExceptionHandler(eh);
    _consumerThread.start();
  }

  public void stop(String msg) {

  }

  public void onStop() {
    if (_consumerThread.isAlive())
      _consumerThread.interrupt();
  }
}
