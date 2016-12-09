package ktsdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import javax.ws.rs.WebApplicationException;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.restexpress.RestExpress;
import org.restexpress.Request;
import org.restexpress.Response;

public class PutResource {
  private static Logger log = LogManager.getLogger(PutResource.class);

  private final Kdb ts;
  private final ObjectMapper mapper;

  public PutResource(Kdb ts) {
    this.ts = ts;
    this.mapper = new ObjectMapper();
  }

  public ByteBuf create(Request request, Response response) {
    response.setResponseCreated();
    return request.getBody();
  }

  /*public Response put(@QueryParam("summary") @DefaultValue("false") BooleanFlag summary,
    @QueryParam("details") @DefaultValue("false") BooleanFlag details,
    @QueryParam("sync") @DefaultValue("false") BooleanFlag sync,
    @QueryParam("sync_timeout") @DefaultValue("0") IntParam sync_timeout,
    JsonNode body) throws Exception {*/
  public String read(Request request, Response response) {
    boolean summary = false; boolean details = false; boolean sync = false; int sync_timeout = 0;
    //log.trace("put; summary: {}, details: {}, sync: {}, sync_timeout: {}, body: {}",
    //          summary, details, sync, sync_timeout, body);

    try {
      ByteBuf buffer = request.getBody();
      byte[] buf = new byte[buffer.readableBytes()];
      buffer.readBytes(buf);
      JsonNode body = mapper.readTree(buf);
      int datapoints = 0;
      List<String> errors = new ArrayList<>();
      Iterator<JsonNode> nodes;
      if (body.isArray()) {
        nodes = body.elements();
      } else {
        nodes = Iterators.singletonIterator(body);
      }
      while (nodes.hasNext()) {
        datapoints++;
        JsonNode node = nodes.next();
        try {
          ts.writeDatapoint(node);
        } catch (JsonProcessingException e) {
          e.printStackTrace();
          errors.add(e.getMessage());
        }
      }

      if (errors.isEmpty()) {
        log.debug("put {} datapoints: {}", datapoints, body);
        return "whatever"; //Response.noContent().build();
      } else {
        log.error("failed to write {} of {} body: {}", errors.size(), datapoints, errors);
      }

    } catch(Exception e) {
      log.info(e);
    } finally {
    }
    return "whatever"; //Response.noContent().build();
  }

  public Response syncPut(boolean summary,
                          boolean details,
                          int timeout,
                          JsonNode datapointNodes) throws Exception {
    int datapoints = 0;
    List<String> errors = new ArrayList<>();
    Iterator<JsonNode> nodes;
    if (datapointNodes.isArray()) {
      nodes = datapointNodes.elements();
    } else {
      nodes = Iterators.singletonIterator(datapointNodes);
    }

    while (nodes.hasNext()) {
      datapoints++;
      JsonNode node = nodes.next();
      try {
        ts.writeDatapoint(node);
      } catch (JsonProcessingException e) {
        errors.add(e.getMessage());
      }
    }
    return null;
  }

  final static class Datapoint {
    private final String metric;
    private final SortedMap<String, String> tags;
    private final long timestamp;
    private final double value;

    @JsonCreator
    public Datapoint(@JsonProperty("metric") String metric,
                     @JsonProperty("tags") SortedMap<String, String> tags,
                     @JsonProperty("timestamp") long timestamp,
                     @JsonProperty("value") double value) {
      this.metric = metric;
      this.tags = tags;
      if (timestamp < 10000000000l) {
        this.timestamp = timestamp * 1000 * 1000;
      } else if (timestamp < 10000000000000l){
        this.timestamp = timestamp * 1000;
      } else {
        throw new WebApplicationException("Illegal timestamp: " + timestamp,
                                          javax.ws.rs.core.Response.Status.BAD_REQUEST);
      }
      this.value = value;
    }

    public String getMetric() {
      return metric;
    }

    public SortedMap<String, String> getTags() {
      return tags;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public double getValue() {
      return value;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
        .add("metric", metric)
        .add("tags", tags)
        .add("timestamp", timestamp)
        .add("value", value)
        .toString();
    }
  }

}
