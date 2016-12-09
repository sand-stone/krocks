package ktsdb;

import java.net.URI;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class PutBench {
  private static Logger log = LogManager.getLogger(PutBench.class);

  public static class Configuration {

    Map<String, Object> params;

    public Map<String, Object> getBench() {
      return params;
    }

    public void setBench(Map<String, Object> params) {
      this.params = params;
    }

  }

  public PutBench() {}

  public static void main(String[] args) {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try {
      Configuration config = mapper.readValue(new File(args[0]), Configuration.class);
      log.info("config {}", ReflectionToStringBuilder.toString(config, ToStringStyle.MULTI_LINE_STYLE));
      new PutBench().run(config);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void run(Configuration config) throws Exception {
    final CloseableHttpClient client = HttpClientBuilder.create().build();

    long start = (long)config.getBench().get("start");
    long end = (long)config.getBench().get("end");
    if (end == 0) end = System.currentTimeMillis();
    if (start == 0) start = end - TimeUnit.HOURS.toMillis(1);
    if (start < 0) start = end - start;

    URI uri = new URIBuilder()
      .setScheme("http")
      .setHost((String)config.getBench().get("ktsdHost"))
      .setPort((int)config.getBench().get("ktsdPort"))
      .setPath("/api/put")
      .setParameter("summary", "true")
      .build();
    List<SortedMap<String, String>> tags = new ArrayList<>();
    tags.add(new TreeMap<String, String>());
    Map<String, List<String>> map = (Map<String, List<String>>)config.getBench().get("tags");
    for (Map.Entry<String, List<String>> tag : map.entrySet()) {
      List<SortedMap<String, String>> original = tags;
      tags = new ArrayList<>();
      for (String value : tag.getValue()) {
        for (SortedMap<String, String> ts : original) {
          SortedMap<String, String> copy = new TreeMap<>(ts);
          copy.put(tag.getKey(), value);
          tags.add(copy);
        }
      }
    }

    CountDownLatch latch = new CountDownLatch(((ArrayList<String>)config.getBench().get("metrics")).size() * tags.size());
    LatencyStats stats = new LatencyStats();

    List<Thread> threads = new ArrayList<>();
    for (String metric : ((ArrayList<String>)config.getBench().get("metrics"))) {
      for (SortedMap<String, String> tagset : tags) {
        DatapointGenerator datapoints = new DatapointGenerator(threads.size(), start, end,
                                                               (int)config.getBench().get("sampleFrequency"));
        threads.add(new Thread(new PutSeries(uri, metric, tagset, datapoints, stats,
                                             client, latch)));
      }
    }

    for (Thread thread : threads) {
      thread.start();
    }

    Histogram hist = stats.getIntervalHistogram();
    while (!latch.await(10, TimeUnit.SECONDS)) {
      Histogram latest = stats.getIntervalHistogram();
      hist.add(latest);

      log.info("Progress:");
      log.info("puts: {}/{}", latest.getTotalCount(), hist.getTotalCount());
      log.info("mean latency: {}/{}", TimeUnit.NANOSECONDS.toMillis((long) latest.getMean()),
               TimeUnit.NANOSECONDS.toMillis((long) hist.getMean()));
      log.info("min: {}/{}", TimeUnit.NANOSECONDS.toMillis(latest.getMinValue()),
               TimeUnit.NANOSECONDS.toMillis(hist.getMinValue()));
      log.info("max: {}/{}", TimeUnit.NANOSECONDS.toMillis(latest.getMaxValue()),
               TimeUnit.NANOSECONDS.toMillis(hist.getMaxValue()));
      log.info("p50: {}/{}", TimeUnit.NANOSECONDS.toMillis(latest.getValueAtPercentile(50)),
               TimeUnit.NANOSECONDS.toMillis(hist.getValueAtPercentile(50)));
      log.info("p99: {}/{}", TimeUnit.NANOSECONDS.toMillis(latest.getValueAtPercentile(99)),
               TimeUnit.NANOSECONDS.toMillis(hist.getValueAtPercentile(99)));
    }

    log.info("Benchmark complete");
    log.info("puts: {}", hist.getTotalCount());
    log.info("mean latency: {}", TimeUnit.NANOSECONDS.toMillis((long) hist.getMean()));
    log.info("stddev: {}", TimeUnit.NANOSECONDS.toMillis((long) hist.getStdDeviation()));
    log.info("min: {}", TimeUnit.NANOSECONDS.toMillis(hist.getMinValue()));
    log.info("max: {}", TimeUnit.NANOSECONDS.toMillis(hist.getMaxValue()));
    log.info("p50: {}", TimeUnit.NANOSECONDS.toMillis(hist.getValueAtPercentile(50)));
    log.info("p90: {}", TimeUnit.NANOSECONDS.toMillis(hist.getValueAtPercentile(90)));
    log.info("p95: {}", TimeUnit.NANOSECONDS.toMillis(hist.getValueAtPercentile(95)));
    log.info("p99: {}", TimeUnit.NANOSECONDS.toMillis(hist.getValueAtPercentile(99)));
    log.info("p999: {}", TimeUnit.NANOSECONDS.toMillis(hist.getValueAtPercentile(99.9)));
  }
}
