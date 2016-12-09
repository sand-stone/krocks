package ktsdb;

import java.util.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import kdb.Client;

public class Tags {
  private static Logger log = LogManager.getLogger(Tags.class);
  private Client client;

  public Tags(Client client) {
    this.client = client;
  }

  public Integer insertTagset(final int id, final SortedMap<String, String> tagset) {
    byte[] v = new byte[]{(byte)0};

    List<byte[]> keys = new ArrayList<>();
    List<byte[]> values = new ArrayList<>();

    for (Map.Entry<String, String> tag : tagset.entrySet()) {
      byte[] key = tag.getKey().getBytes();
      byte[] value = tag.getKey().getBytes();
      ByteBuffer buf = ByteBuffer.allocate(key.length+value.length+4).order(ByteOrder.BIG_ENDIAN);
      buf.put(key).put(value).putInt(id);
      keys.add(key);
      values.add(v);
    }
    client.put(keys, values);
    return id;
  }

  public IntVec getTagsetIDsForTag(final String key, final String value) {
    IntVec tagsetIDs = IntVec.create();
    byte[] k = key.getBytes();
    byte[] v = value.getBytes();
    byte[] prefix = ByteBuffer
      .allocate(k.length+v.length)
      .order(ByteOrder.BIG_ENDIAN)
      .put(k)
      .put(v)
      .array();
    byte[] suffix = ByteBuffer
      .allocate(k.length+v.length+1)
      .order(ByteOrder.BIG_ENDIAN)
      .put(k)
      .put(v)
      .put((byte)0xFF)
      .array();
    Client.Result rsp = client.scanForward(prefix, suffix, 10);
    do {
      int count = rsp.count();
      if(count == 0)
        break;
      for(int i = 0; i < rsp.count(); i++) {
        tagsetIDs.push(Tables.toInt(rsp.getValue(i)));
      }
      if(rsp.token().length() > 0) {
        rsp = client.scanNext(10);
      }
    } while(true);
    return tagsetIDs;
  }

  public IntVec getTagsetIDsForTags(Map<String, String> tags) {
    List<IntVec> idset = new ArrayList<>(tags.size());
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      idset.add(getTagsetIDsForTag(tag.getKey(), tag.getValue()));
    }
    IntVec intersection = idset.remove(idset.size() - 1);
    for (IntVec ids : idset) {
      intersection.intersect(ids);
    }
    intersection.dedup();
    return intersection;
  }

}
