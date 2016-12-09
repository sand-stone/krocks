package ktsdb;


import java.util.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import kdb.Client;

public class Kdb implements AutoCloseable {

  private static Logger log = LogManager.getLogger(Kdb.class);

  private final Metrics metrics;
  private final Tagsets tagsets;
  private final Tags tags;
  private String uri;
  private String name;

  public Kdb(String uri, String name, Metrics metrics, Tagsets tagsets, Tags tags) {
    this.name = name;
    this.uri = uri;
    this.metrics = metrics;
    this.tags = tags;
    this.tagsets = tagsets;
    //Tables.createTables(uri, name);
  }

  public static Kdb open(List<String> uris, String name) throws Exception {
    Tags tags = new Tags(createClient(uris.get(0), Tables.tagsTableName(name)));
    Tagsets tagsets = new Tagsets(createClient(uris.get(0), Tables.tagsetsTableName(name)), tags);
    Metrics metrics = new Metrics(createClient(uris.get(0), Tables.metricsTableName(name)), tagsets);
    return new Kdb(uris.get(0), name, metrics, tagsets, tags);
  }

  private static Client createClient(String uri, String name) {
    Client client = new Client(uri, name);
    //client.open();
    return client;
  }

  public void writeDatapoint(JsonNode json) throws JsonProcessingException {
    log.info("writeDatapoint {}", json);
  }

  public void writeDatapoint(final String metric,
                             SortedMap<String, String> tags,
                             final long time,
                             final double value) {
    int tagsetID = tagsets.getTagsetID(tags);
    metrics.insertDatapoint(metric, tagsetID, time, value);
  }

  public List<Datapoints> query(final Query query) throws Exception {
    IntVec tagsetIDs = tags.getTagsetIDsForTags(query.getTags());
    IntVec.Iterator iter = tagsetIDs.iterator();
    List<Datapoints> series = new ArrayList<>(tagsetIDs.len());
    while (iter.hasNext()) {
      series.add(metrics.scanSeries(query.getMetric(),
                                    iter.next(),
                                    query.getStart(),
                                    query.getEnd(),
                                    query.getDownsampler(),
                                    query.getDownsampleInterval()));
    }

    // Filter empty series
    Iterator<Datapoints> seriesIter = series.iterator();
    while (seriesIter.hasNext()) {
      if (seriesIter.next().size() == 0) seriesIter.remove();
    }

    if (series.isEmpty()) { return ImmutableList.of(); }

    if (query.getInterpolator() == null) {
      return ImmutableList.of(Datapoints.aggregate(series, query.getAggregator()));
    } else {
      return ImmutableList.of(Datapoints.aggregate(series, query.getAggregator(),
                                                   query.getInterpolator()));
    }
  }

  public void flush() {

  }

  Metrics metrics() {
    return metrics;
  }

  Tags tags() {
    return tags;
  }

  Tagsets tagsets() {
    return tagsets;
  }

  public void close() throws Exception {
  }

  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).toString();
  }

}
