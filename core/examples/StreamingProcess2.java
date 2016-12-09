import org.rocksdb.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.LocalTime;

public class StreamingProcess2 {

  private static boolean stop;
  private static String[] uris;

  private static UUID[] deviceIds;

  private static int numT = 12;

  private static void init() {
    //deviceIds = new UUID[150000000];
    //deviceIds = new UUID[100000];
    deviceIds = new UUID[1000];
    for(int i = 0; i < deviceIds.length; i++) {
      deviceIds[i] = UUID.randomUUID();
    }
  }

  public static class EventSource implements Runnable {
    private int r1, r2;
    private Random rnd;
    private int valSize;

    public EventSource(int r1, int r2) {
      this.r1  = r1;
      this.r2  = r2;
      rnd = new Random();
      valSize = 300;
    }

    private void deviceid(ByteBuffer buf) {
      UUID guid = deviceIds[rnd.nextInt(deviceIds.length)];
      buf.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
    }

    public void run() {
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      int batch = 0;
      int batchSize;
      int total = 0;
      long t1 = System.nanoTime();
      for(int i = r1; i < r2; i++) {
        for(int j = 0; j < numT; j++) {
          for (int m = 0; m < deviceIds.length; m++) {
            //int deviceid = rnd.nextInt(deviceIds.length);
            for(int k = 0; k < 6; k++) {
              ByteBuffer key = ByteBuffer.allocate(19).order(ByteOrder.BIG_ENDIAN);
              key.put((byte)i);
              key.put((byte)j);
              UUID guid = deviceIds[m];
              key.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
              //deviceid(key);
              key.put((byte)k);
              keys.add(key.array());
              byte[] value = new byte[valSize];
              rnd.nextBytes(value);
              values.add(value);
            }
            if(m%1000 == 0) {
              batchSize = keys.size();
              updates(events, keys, values);
              keys.clear();
              values.clear();
              total += batchSize;
              long t2 = System.nanoTime();
              if(m%10000 == 0)
                System.out.printf("eventsource %d bucket %d:%d batchSize %d total %d events takes %e seconds, rate %e \n",
                                  Thread.currentThread().getId(), i, j, batchSize, total,(t2-t1)/1e9, total/((t2-t1)/1e9));
            }
          }
        }
      }
      System.out.printf("eventsource inserted %d events\n", total);
    }
  }

  public static class QueryState implements Runnable {
    private int id;
    private Random rnd;
    private int numquery;

    public QueryState(int id) {
      this.id = id;
      rnd = new Random();
      numquery = 7000;
    }

    private int process(List<byte[]> eventskeys, List<byte[]> eventsvalues) {
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      byte[] k = null; int joincount = 0;
      for(int i = 0; i < eventskeys.size(); i++) {
        if(k == null || memcmp(k, eventskeys.get(i), 18) != 0) {
          k = eventskeys.get(i);
          keys.add(Arrays.copyOf(k, k.length));
          byte[] value = new byte[numquery*256/8];
          rnd.nextBytes(value);
          values.add(value);
        } else {
          joincount++;
        }
      }
      updates(states, keys, values);
      System.out.println("bucket " + id + " eventkeys size:" + eventskeys.size() + " stateskey size:" + keys.size());
      return values.size();
    }

    public void run() {
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      long t1 = System.nanoTime();
      int b1 = id;
      System.out.printf("state processor %d bucket %d:* \n", Thread.currentThread().getId(), id);

      while(!stop) {
        for(int b2 = 0; b2 < 1; b2++) {
          ByteBuffer key1 = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
          key1.put((byte)b1).put((byte)b2);
          ByteBuffer key2 = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
          key2.put((byte)(b1)).put((byte)(b2+1));
          int batch = 100000;
          RocksIterator cursor = rangeCursor(events, key1.array());
          byte[] suffix = key2.array();
          int count1 = 0, count2 = 0;
          do {
            scan(cursor, suffix, keys, values, batch);
            count1 += keys.size();
            if(keys.size() == 0)
              break;
            count2 += process(keys, values);
            keys.clear();
            values.clear();
          } while(cursor.isValid());
          cursor.close();
          if(count1 != 0 && count2 != 0)
            System.out.println("processed bucket " + id + ":" + b2 + " events count:" + count1 + " states :" + count2);
        }
      }
    }
  }

  static RocksDB createDB(String name) {
    Options options = new Options().setCreateIfMissing(true);
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

  static void updates(RocksDB db, List<byte[]> keys, List<byte[]> values) {
    WriteOptions writeOpts = new WriteOptions();
    WriteBatch writeBatch = new WriteBatch();
    try {
      for(int i = 0; i < keys.size(); i++) {
        writeBatch.put(keys.get(i), values.get(i));
      }
      db.write(writeOpts, writeBatch);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      writeOpts.dispose();
      writeBatch.dispose();
    }
  }

  private static RocksDB events;
  private static RocksDB states;

  public static void main(String[] args) {
    uris = args;
    RocksDB.loadLibrary();
    init();

    events = createDB("events");
    states = createDB("states");

    new Thread(new EventSource(0, 6)).start();
    new Thread(new EventSource(6, 12)).start();

    System.out.println("event source threads");

    try {Thread.currentThread().sleep(5000);} catch(Exception ex) {}

    for (int i = 0; i < numT; i++) {
      new Thread(new QueryState(i)).start();
    }

    try {Thread.currentThread().sleep(60*60*1000);} catch(Exception ex) {}
    stop = true;
  }
}
