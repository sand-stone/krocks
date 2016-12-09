package ktsdb;

import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import kdb.Client;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * {@code Tagsets} manages looking up tagset IDs and tagsets in the
 * {@code tagsets} table. Tagsets and IDs are cached internally, so that
 * subsequent lookups on the same tagset or ID are fast. If a tagset isn't found
 * during a lookup, it is automatically inserted into the {@code tagsets} table,
 * and its tags are inserted into the {@code tags} table.
 *
 * To guarantee that tagset IDs are unique, the {@code tagsets} table is
 * structured as a linear-probe hash table. The tagset is transformed into a
 * canonical byte representation using an internal protobuf format, and the hash
 * of this canonical value is used as the tagset ID. On ID collision, linear
 * probing is used to find a new ID.
 *
 * Internally, {@code Tagsets} keeps an LRU cache of tagsets and IDs so that
 * lookups of frequently used tagsets are fast.
 *
 * Steps for looking up a new tagset:
 *
 *  1) the tagset is converted to a canonical byte string format
 *     (see {@link SerializedTagset}).
 *  2) the internal LRU cache is queried with the byte string, but the lookup fails.
 *  3) a hash of the tagset's byte string is created with the MurmurHash3_32
 *     algorithm (see {@link SerializedTagset#hashCode}).
 *  4) up to {@link #TAGSETS_PER_SCAN} tagsets are scanned from the {@code tagsets}
 *     table beginning with the computed hash as the ID.
 *  5) the tagsets returned in the scan are checked in ID order. If the tagset
 *     is found, the corresponding ID is returned. If there is an ID missing
 *     in the results, then the tagset is inserted with that ID (go to step 6).
 *     If {@link #TAGSETS_PER_SCAN} IDs are present, but the tagset isn't found,
 *     then a new scan is started (step 4), but using the next unscanned ID as
 *     the start ID.
 *  6) the ID from step 5 is used to insert the rowset into the {@code rowsets}
 *     table. If the insert results in a duplicate primary key error, then
 *     another client has concurrently inserted a rowset using the ID. The
 *     concurrently inserted rowset may or may not match the rowset we tried to
 *     insert, so we return to step 4 using the duplicate ID as the start ID.
 *  7) After inserting the tagset successfully in step 6, every tag in the
 *     tagset is inserted into the {@code tags} table. No duplicate errors are
 *     expected in this step.
 *
 * Tagset IDs are 32bits, which allows for hundreds of millions of unique tagsets
 * without risking excessive hash collisions.
 */
public class Tagsets {
  private static Logger log = LogManager.getLogger(Tagsets.class);
  private static final int TAGSETS_PER_SCAN = 10;

  private Tags tags;
  private final LoadingCache<SerializedTagset, Integer> tagsets;

  private Client client;

  public Tagsets(Client client, Tags tags) {
    this.tags = tags;
    this.tagsets = CacheBuilder.newBuilder()
      .maximumSize(1024 * 1024)
      .build(new CacheLoader<SerializedTagset, Integer>() {
          @Override
          public Integer load(SerializedTagset tagset) {
            return lookupOrInsertTagset(tagset, tagset.hashCode());
          }
        });
    this.client = client;
  }

  public int getTagsetID(SortedMap<String, String> tagset) {
    return tagsets.getUnchecked(new SerializedTagset(tagset));
  }

  private void insertTagset(final SerializedTagset tagset, final int id) {
    client.put(Arrays.asList(Tables.toBytes(id)), Arrays.asList(tagset.getBytes()));
  }

  private Integer lookupOrInsertTagset(final SerializedTagset tagset, final int id) {
    byte[] prefix = ByteBuffer
      .allocate(4)
      .order(ByteOrder.BIG_ENDIAN)
      .putInt(id)
      .array();
    Client.Result rsp = client.scanForward(prefix, TAGSETS_PER_SCAN);
    int probe = id; boolean stop = false; int ret = -1;
    do {
      for(int i = 0; i < rsp.count(); i++) {
        if(Tables.toInt(rsp.getKey(i)) != probe) {
          stop = true;
          insertTagset(tagset, probe);
          tags.insertTagset(probe, tagset.deserialize());
          ret = probe;
          break;
        }
        if (tagset.equals(rsp.getValue(i))) {
          stop = true;
          ret = id;
          break;
        }
        probe++;
      }
      if(rsp.token().length() == 0) {
        insertTagset(tagset, probe);
        tags.insertTagset(probe, tagset.deserialize());
        ret = probe;
        break;
      }
      rsp = client.scanNext(TAGSETS_PER_SCAN);
    } while(!stop);

    return ret;
  }

  void clear() {
    tagsets.invalidateAll();
  }

  /**
   * Serializes a set of tags into a canonical byte format for storing in the
   * {@code tagsets} table. The format consists of the sorted sequence of key
   * and value strings serialized with a leading two byte length header, then
   * the UTF-8 encoded bytes.
   *
   * The hash of the tagset is computed over the serialized bytes using the
   * Murmur3_32 algorithm.
   */
  @VisibleForTesting
  static class SerializedTagset {
    private final byte[] bytes;

    public SerializedTagset(SortedMap<String, String> tagset) {
      try {
        int lengthEstimate = 0;
        for (Map.Entry<String, String> tags : tagset.entrySet()) {
          lengthEstimate += tags.getKey().length() + tags.getValue().length() + 4;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream(lengthEstimate);
        DataOutputStream dos = new DataOutputStream(baos);

        for (Map.Entry<String, String> tag : tagset.entrySet()) {
          dos.writeUTF(tag.getKey());
          dos.writeUTF(tag.getValue());
        }
        dos.flush();
        baos.flush();
        bytes = baos.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException("unreachable");
      }
    }

    /**
     * Returns the serialized tagset. The caller must not modify the array.
     * @return the serialized tagset
     */
    public byte[] getBytes() {
      return bytes;
    }

    public SortedMap<String, String> deserialize() {
      try {
        SortedMap<String, String> tags = new TreeMap<>();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        while (dis.available() > 0) {
          tags.put(dis.readUTF(), dis.readUTF());
        }
        return tags;
      } catch (IOException e) {
        throw new RuntimeException("unreachable");
      }
    }

    public boolean equals(byte[] buf) {
      return Arrays.equals(bytes, buf);
    }

    public boolean equals(ByteBuffer buf) {
      if (buf.limit() - buf.position() != bytes.length) return false;

      for (int i = 0, j = buf.position(); i < bytes.length; i++, j++) {
        if (bytes[i] != buf.get(j)) return false;
      }
      return true;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null) return false;
      else if (o.getClass() != getClass()) return false;
      SerializedTagset that = (SerializedTagset) o;
      return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
      return Hashing.murmur3_32().hashBytes(bytes).asInt();
    }

    @Override
    public String toString() {
      return deserialize().toString();
    }
  }

}
