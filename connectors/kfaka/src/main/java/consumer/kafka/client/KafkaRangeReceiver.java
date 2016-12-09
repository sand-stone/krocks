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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Iterator;

import consumer.kafka.KafkaConfig;
import consumer.kafka.KafkaConsumer;
import consumer.kafka.MessageAndMetadata;
import consumer.kafka.Receiver;
import consumer.kafka.ZkState;
import consumer.kafka.KDBContext;

@SuppressWarnings("serial")
public class KafkaRangeReceiver extends Receiver {

  private Set<Integer> _partitionSet;
  private List<Thread> _threadList = new ArrayList<Thread>();

  public KafkaRangeReceiver(KDBContext ctx, Properties props, Set<Integer> partitionSet) {
    super(ctx);
    this._props = props;
    _partitionSet = partitionSet;
  }

  public void onStart() {
    start();
  }

  public void restart(String msg, Throwable ex, int delay) {

  }

  public void start() {

    // Start the thread that receives data over a connection
    _threadList.clear();
    KafkaConfig kafkaConfig = new KafkaConfig(_props);

    for (Integer partitionId : _partitionSet) {
      ZkState zkState = new ZkState(kafkaConfig);
      _kConsumer = new KafkaConsumer(kafkaConfig, zkState, this);
      _kConsumer.open(partitionId);
      Thread.UncaughtExceptionHandler eh =
        new Thread.UncaughtExceptionHandler() {
          public void uncaughtException(Thread th, Throwable ex) {
            if (ex instanceof InterruptedException) {
              th.interrupt();
              stop(" Stopping Receiver due to " + ex);
            } else {
              restart("Restarting Receiver ", ex, 5000);
            }
          }
        };
      _consumerThread = new Thread(_kConsumer);
      _consumerThread.setDaemon(true);
      _consumerThread.setUncaughtExceptionHandler(eh);
      _threadList.add(_consumerThread);
      _consumerThread.start();
    }
  }

  @Override
  public void onStop() {
    for (Thread t : _threadList) {
      if (t.isAlive())
        t.interrupt();
    }
  }
}
