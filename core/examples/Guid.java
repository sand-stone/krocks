import org.rocksdb.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.LocalTime;

public class Guid {

  static RocksDB createDB(String name) {
    Options options = new Options().setCreateIfMissing(true);
    options.setAllowConcurrentMemtableWrite(true);
    options.setEnableWriteThreadAdaptiveYield(true);
    RocksDB db = null;
    try {
      db = RocksDB.open(options, name);
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
    return db;
  }

  static int memcmp(final byte[] a, final byte[] b, int len) {
    for (int i = 0; i < len; i++) {
      if (a[i] != b[i]) {
        return (a[i] & 0xFF) - (b[i] & 0xFF);
      }
    }
    return 0;
  }

  static RocksIterator rangeCursor(RocksDB db, byte[] prefix) {
    ReadOptions readOptions = new ReadOptions();
    readOptions.setTotalOrderSeek(true);
    readOptions.setPrefixSameAsStart(true);
    RocksIterator cursor = db.newIterator(readOptions);
    cursor.seek(prefix);
    return cursor;
  }

  static void scan(RocksIterator cursor, byte[] suffix, List<byte[]> keys, List<byte[]> values, int limit) {
    int count = 0;
    while(cursor.isValid()) {
      byte[] key = cursor.key();
      byte[] value = cursor.value();
      if(memcmp(key, suffix, suffix.length) < 0) {
        keys.add(Arrays.copyOf(key, key.length));
        values.add(Arrays.copyOf(value, value.length));
      } else {
        break;
      }
      cursor.next();
      if(count++ > limit)
        return;
    }
  }

  static void put(RocksDB db, int start, int end, List<byte[]> keys, List<byte[]> values) {
    try(WriteOptions writeOpts = new WriteOptions();
        WriteBatch writeBatch = new WriteBatch()) {
      for(int i = start; i < end; i++) {
        writeBatch.put(keys.get(i), values.get(i));
      }
      db.write(writeOpts, writeBatch);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    RocksDB.loadLibrary();

    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();

    Random rnd = new Random();
    byte[] valueState = new byte[7000*8];
    rnd.nextBytes(valueState);

    for(int i = 0; i < 100000; i++) {
      UUID guid = UUID.randomUUID();
      ByteBuffer key = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
      key.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
      keys.add(key.array());
      values.add(Arrays.copyOf(valueState, valueState.length));
    }

    RocksDB acme = createDB("acme");
    int i = 0;
    for (i = 0; i + 100 < keys.size(); i+= 100) {
      put(acme, i, i+100, keys, values);
    }

    if(i < keys.size()) {
      put(acme, i, keys.size(), keys, values);
    }

  }

}
