package ktsdb;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import java.util.Arrays;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import kdb.Client;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class Metrics {

  private static Logger log = LogManager.getLogger(Metrics.class);

  private final Client client;
  private final Tagsets tagsets;

  public Metrics(Client client, Tagsets tagsets) {
    this.client = client;
    this.tagsets = tagsets;
  }

  public void insertDatapoint(final String metric,
                              final int tagsetID,
                              final long time,
                              final double value) {

    byte[] m = metric.getBytes();
    byte[] key = ByteBuffer
      .allocate(m.length + 4 + 8)
      .order(ByteOrder.BIG_ENDIAN)
      .put(m)
      .putInt(tagsetID)
      .putLong(time)
      .array();
    client.put(Arrays.asList(key), Arrays.asList(Tables.toBytes(value)));
  }

  public Datapoints scanSeries(final String metric,
                               final int tagsetID,
                               final long startTime,
                               final long endTime,
                               final Aggregator downsampler,
                               final long downsampleInterval) {
    LongVec times = LongVec.create();
    DoubleVec values = DoubleVec.create();
    byte[] m = metric.getBytes();
    byte[] prefix = ByteBuffer
      .allocate(m.length + 4 + 8)
      .order(ByteOrder.BIG_ENDIAN)
      .put(m)
      .putInt(tagsetID)
      .putLong(startTime)
      .array();
    byte[] suffix = ByteBuffer
      .allocate(m.length + 4 + 8)
      .order(ByteOrder.BIG_ENDIAN)
      .put(m)
      .putInt(tagsetID)
      .putLong(endTime)
      .array();
    Client.Result rsp = client.scanForward(prefix, suffix, 10);
    ByteBuffer ts = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
    if (downsampler == null) {
      do {
        for(int i = 0; i < rsp.count(); i++) {
          byte[] k = rsp.getKey(i);
          times.push(ts.put(k, k.length - 8, 8).getLong());
          ts.rewind();
          values.push(Tables.toDouble(rsp.getValue(i)));
        }
        if(rsp.token().length() == 0)
          break;
        rsp = client.scanNext(10);
      } while(true);
    } else {
      long currentInterval = 0;
      int valuesInCurrentInterval = 0;
      do {
        for(int i = 0; i < rsp.count(); i++) {
          byte[] k = rsp.getKey(i);
          long time = ts.put(k, k.length - 8, 8).getLong();
          ts.rewind();
          long interval = time - (time % downsampleInterval);
          double value = Tables.toDouble(rsp.getValue(i));
          if (interval == currentInterval) {
            downsampler.addValue(value);
            valuesInCurrentInterval++;
          } else {
            if (valuesInCurrentInterval != 0) {
              times.push(currentInterval);
              values.push(downsampler.aggregatedValue());
              valuesInCurrentInterval = 0;
            }
            currentInterval = interval;
            valuesInCurrentInterval++;
            downsampler.addValue(value);
          }
        }
        if(rsp.token().length() == 0)
          break;
        rsp = client.scanNext(10);
      } while(true);
      if (valuesInCurrentInterval != 0) {
        times.push(currentInterval);
        values.push(downsampler.aggregatedValue());
      }
    }
    return new Datapoints(metric,
                          IntVec.wrap(new int[] { tagsetID }),
                          times,
                          values);
  }

}
