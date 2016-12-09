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
import java.util.Properties;
import java.util.Set;
import java.util.Iterator;
import java.util.UUID;
import consumer.kafka.KafkaConfig;
import consumer.kafka.KafkaConsumer;
import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ZkState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;

public abstract class Receiver {
  public static final Logger LOG = LoggerFactory.getLogger(Receiver.class);
  private Gson gson;
  protected Properties _props = null;
  protected KafkaConsumer _kConsumer;
  protected transient Thread _consumerThread;
  protected KDBContext ctx;

  public static class KV {
    public String id;
    public String value;
  }

  public Receiver(KDBContext ctx) {
    this.ctx = ctx;
    this.gson = new Gson();
  }

  public void onStart() {
    start();
  }


  public abstract void start();

  public abstract void onStop();

  public boolean isStopped() {
    return false;
  }

  public void stop(String msg) {}

  public abstract void restart(String msg, Throwable ex, int delay);

  private void dump(MessageAndMetadata msg) {
    try {
      LOG.info(msg.getOffset() + ": " + new String(msg.getPayload(), "UTF-8"));
    } catch (Exception ex) {
      LOG.info("ex:{}", ex);
    }
  }

  public void store(List<MessageAndMetadata> msgs) {
    LOG.info("Kafka receiver store"+msgs);
    for(MessageAndMetadata msg : msgs) {
      dump(msg);
      ctx.upsertData(UUID.randomUUID().toString(), msg.getPayload());
    }
  }

  public void reportError(String msg, Throwable ex) {
    LOG.info(msg+ex.toString());
  }
}
