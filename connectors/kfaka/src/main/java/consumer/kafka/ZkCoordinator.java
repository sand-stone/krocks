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

/*
 *   This file is based on the source code of the Kafka spout of the Apache Storm project.
 *   (https://github.com/apache/storm/tree/master/external/storm-kafka)
 */

package consumer.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import consumer.kafka.client.KafkaReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class ZkCoordinator implements PartitionCoordinator, Serializable {
  public static final Logger LOG = LoggerFactory.getLogger(ZkCoordinator.class);

  private KafkaConfig _kafkaconfig;
  private int _partitionOwner;
  private Map<Partition, PartitionManager> _managers =
    new HashMap<Partition, PartitionManager>();
  private List<PartitionManager> _cachedList;
  private Long _lastRefreshTime = 0L;
  private int _refreshFreqMs;
  private DynamicPartitionConnections _connections;
  private DynamicBrokersReader _reader;
  private GlobalPartitionInformation _brokerInfo;
  private KafkaConfig _config;
  private Receiver _receiver;
  private boolean _restart;

  public ZkCoordinator(
                       DynamicPartitionConnections connections,
                       KafkaConfig config,
                       ZkState state,
                       int partitionId,
                       Receiver receiver,
                       boolean restart) {
    _kafkaconfig = config;
    _connections = connections;
    _partitionOwner = partitionId;
    _refreshFreqMs = config._refreshFreqSecs * 1000;
    _reader = new DynamicBrokersReader(_kafkaconfig, state);
    _brokerInfo = _reader.getBrokerInfo();
    _config = config;
    _receiver = receiver;
    _restart = restart;
  }

  @Override
  public List<PartitionManager> getMyManagedPartitions() {
    if ((System.currentTimeMillis() - _lastRefreshTime) > _refreshFreqMs) {
      refresh();
      _lastRefreshTime = System.currentTimeMillis();
    }
    _restart = false;
    return _cachedList;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void refresh() {
    try {
      LOG.info("Refreshing partition manager connections");
      _brokerInfo = _reader.getBrokerInfo();
      Set<Partition> mine = new HashSet<Partition>();
      for (Partition partition : _brokerInfo) {
        if (partition.partition == _partitionOwner) {
          mine.add(partition);
          LOG.debug("Added partition index {} for coordinator", _partitionOwner);
        }
      }
      if (mine.size() == 0) {
        LOG.warn("Some issue getting Partition details.Patrition Manager size Zero");
        _managers.clear();
        if (_cachedList != null) {
          _cachedList.clear();
        }
        return;
      } else {
        Set<Partition> curr = _managers.keySet();
        Set<Partition> newPartitions = new HashSet<Partition>(mine);
        newPartitions.removeAll(curr);
        Set<Partition> deletedPartitions = new HashSet<Partition>(curr);
        deletedPartitions.removeAll(mine);
        LOG.debug("Deleted partition managers: {}", deletedPartitions.toString());

        for (Partition id : deletedPartitions) {
          PartitionManager man = _managers.remove(id);
          man.close();
        }
        LOG.debug("New partition managers {}", newPartitions.toString());

        // Try to get the latest Fill Rate
        ZkState state = null;
        try {
          state = new ZkState((String)_config._stateConf.get(Config.ZOOKEEPER_CONSUMER_CONNECTION));
          Map<Object, Object> rateJson = state.readJSON(ratePath());
          if (rateJson != null) {
            String conId = (String)((Map<Object, Object>) rateJson
                                    .get("consumer")).get("id");
            if (conId != null && 
                conId.equalsIgnoreCase((String) _config._stateConf.get(Config.KAFKA_CONSUMER_ID))) {
              int newFetchSize = ((Long) rateJson.get("rate")).intValue();
              LOG.info("Modified Fetch Rate for topic {} to : {}",
                       _config._stateConf.get(Config.KAFKA_TOPIC),newFetchSize);
              _kafkaconfig._fetchSizeBytes = newFetchSize;
              _kafkaconfig._bufferSizeBytes = newFetchSize * 2;
            }
          }
        } catch (Throwable e) {
          LOG.error("Error reading and/or parsing at ZkNode", e);
        } finally {
          if (state != null)
            state.close();
        }

        for (Partition id : newPartitions) {
          PartitionManager man = new PartitionManager(_connections,
                                                      new ZkState((String) _config._stateConf.get(Config.ZOOKEEPER_CONSUMER_CONNECTION)),
                                                      _kafkaconfig,
                                                      id,
                                                      _receiver,
                                                      _restart);
          _managers.put(id, man);
        }

        _cachedList = new ArrayList<PartitionManager>(_managers.values());
        LOG.info("Finished refreshing");
      }
    } catch (Exception e) {
      throw new FailedFetchException(e);
    }
  }

  @Override
  public PartitionManager getManager(Partition partition) {
    return _managers.get(partition);
  }

  public String ratePath() {
    return _config._stateConf.get(Config.ZOOKEEPER_CONSUMER_PATH)
      + "/" + _config._stateConf.get(Config.KAFKA_CONSUMER_ID)
      + "/" + _config._stateConf.get(Config.KAFKA_TOPIC) + "/newrate";
  }
}
