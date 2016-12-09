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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.Map;
import java.io.FileInputStream;

import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ProcessedOffsetManager;
import consumer.kafka.ReceiverLauncher;
import consumer.kafka.KDBContext;

@SuppressWarnings("serial")
public class SampleConsumer implements Serializable {
  String config;
  public void start(String config) throws InstantiationException, IllegalAccessException,
                                          ClassNotFoundException {
    this.config = config;
    run();
  }

  @SuppressWarnings("deprecation")
  private void run() {
    Properties props = new Properties();
    InputStream input = null;
    try {
      input = new FileInputStream(config);
      props.load(input);
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    // Specify number of Receivers you need.
    int numberOfReceivers = 1;
    KDBContext ctx = new KDBContext(props);
    ReceiverLauncher.launch(ctx, props, numberOfReceivers);

    //Get the Max offset from each RDD Partitions. Each RDD Partition belongs to One Kafka Partition
    Map<Integer, Iterable<Long>> partitonOffset = ProcessedOffsetManager
      .getPartitionOffset(ctx);

    ctx.start();
    ctx.awaitTermination();
    ProcessedOffsetManager.persists(partitonOffset, props);
  }

  public static void main(String[] args) throws Exception {
    if(args.length<1) {
      System.out.println("java classPath sampleConsumer config");
      return;
    }
    SampleConsumer consumer = new SampleConsumer();
    consumer.start(args[0]);
  }
}
