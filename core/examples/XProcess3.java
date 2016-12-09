import kdb.Client;
import kdb.KdbException;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.LocalTime;

public class XProcess3 {

  private static String events = "xevents";
  private static String states = "xstates";

  private static String[] uris;

  private final static int hours = 6;
  private final static int min5 = 12;

  public static class EventSource implements Runnable {
    private int id;
    private Random rnd;
    private int valSize;
    private UUID[] deviceIds;


    public EventSource(int id) {
      this.id  = id;
      rnd = new Random();
      valSize = 300;
    }

    private void init() {
      deviceIds = new UUID[100000]; //150M per hour, shard by 100, 1M per shard per hour, 100K per 5 min bar
      //deviceIds = new UUID[50000]; //150M per hour, shard by 100, 1M per shard per hour, 100K per 5 min bar
      for(int i = 0; i < deviceIds.length; i++) {
        deviceIds[i] = UUID.randomUUID();
      }
    }

    public void run() {
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      int batchSize = 100;
      int rounds = 100;
      while(rounds -- > 0) {
        init();
        int hcount = 0;
        int total = 0;
        int mtotal, htotal;
        try (Client client = new Client("http://localhost:8000/", events)) {
          client.open();
          while (hcount < hours) {
            int mcount = 0;
            long htime = System.nanoTime();
            htotal = 0;
            while(mcount < min5) {
              long mtime = System.nanoTime();
              mtotal = 0;
              keys.clear();
              values.clear();
              for(int i = 0; i < deviceIds.length; i++) {
                UUID guid = deviceIds[i];
                for(int k = 0; k < 6; k++) {
                  ByteBuffer key = ByteBuffer.allocate(19).order(ByteOrder.BIG_ENDIAN);
                  key.put((byte)hcount);
                  key.put((byte)mcount);
                  key.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
                  keys.add(key.array());
                  byte[] value = new byte[valSize];
                  rnd.nextBytes(value);
                  values.add(value);
                }
                if(keys.size() >= batchSize) {
                  client.put(keys, values);
                  total += keys.size();
                  mtotal += keys.size();
                  htotal += keys.size();
                  keys.clear();
                  values.clear();
                }
              }
              if(keys.size() > 0) {
                client.put(keys, values);
                total += keys.size();
                mtotal += keys.size();
                htotal += keys.size();
              }
              //System.out.printf("eventsource %d h %d m % d inserted %d events\n", id, hcount, mcount, total);
              double emtime = (System.nanoTime()-mtime)/1e9;
              System.out.printf("eventsource %d h %d m % d inserted %d events in %e seconds avg %e \n", id, hcount, mcount, mtotal,
                                emtime, mtotal/emtime);
              mcount++;
            }
            double hetime = (System.nanoTime()-htime)/1e9;
            System.out.printf("eventsource %d h %d inserted %d events in %e seconds avg %e \n", id, hcount, htotal,
                              hetime, htotal/hetime);
            //try {Thread.currentThread().sleep(30000);} catch(Exception ex) {}
            hcount++;
          }
          //try {Thread.currentThread().sleep(300000);} catch(Exception ex) {}
          System.out.printf("round %d eventsource %d inserted %d events\n", rounds, id, total);
        } catch(KdbException e) {
          System.out.printf("timeout round %d eventsource %d inserted %d total \n", rounds, id, total);
        }
      }
    }
  }

  public static int memcmp(final byte[] a, final byte[] b, int len) {
    for (int i = 0; i < len; i++) {
      if (a[i] != b[i]) {
        return (a[i] & 0xFF) - (b[i] & 0xFF);
      }
    }
    return 0;
  }

  public static class Query implements Runnable {
    private int m5;
    private Random rnd;
    private byte[] valueState;

    public Query(int m5) {
      this.m5  = m5;
      rnd = new Random();
      valueState = new byte[7000*8];
      rnd.nextBytes(valueState);
    }

    private void process(Client client, int hcount, List<byte[]> eventKeys) {
      if(eventKeys.size() == 0)
        return;
      long t1 = System.nanoTime();
      int batchSize = 1000;
      List<byte[]> values = new ArrayList<byte[]>();
      List<byte[]> keys;
      int index = 0; int step = 100;
      int pcount = 0;
      int existedKeys  = 0;
      for(index = 0; (index + step) < eventKeys.size(); index += step) {
        keys = eventKeys.subList(index, index+step);
        Client.Result rsp = client.get(keys);
        existedKeys += rsp.count();
        for(int i = 0; i < keys.size(); i++) {
          values.add(Arrays.copyOf(valueState, valueState.length));
        }
        long s1 = System.nanoTime();
        client.put(keys, values);
        long s2 = System.nanoTime();
        pcount += keys.size();
        System.out.printf("state writing h %d m %d keys %d out of %d in %e seconds \n", hcount, m5, pcount, eventKeys.size(), (s2-s1)/1e9);
        values.clear();
      }

      if(index < eventKeys.size()) {
        values.clear();
        keys = eventKeys.subList(index, eventKeys.size());
        for(int i = 0; i < keys.size(); i++) {
          byte[] value = new byte[7000*8];
          rnd.nextBytes(value);
          values.add(value);
        }
        pcount += keys.size();
        client.put(keys, values);
      }
      long t2 = System.nanoTime();
      //System.out.printf("query worker hcount %d mcount %d commit %d took %e seconds avg %e existedKeys %d \n", hcount, m5, pcount, (t2-t1)/1e9, pcount/((t2-t1)/1e9), existedKeys);
    }

    public void run() {
      int k = 1000;
      while (k--> 0) {
        int hcount = 0;
        int total = 0;
        while (hcount < hours) {
          try (Client stateClient = new Client("http://localhost:8000/", states+m5)) {
            stateClient.openCompressed("snappy");
            ByteBuffer key1 = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
            key1.put((byte)hcount).put((byte)m5);
            ByteBuffer key2 = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
            key2.put((byte)hcount).put((byte)(m5+1));
            List<byte[]> keys = new ArrayList<byte[]>();
            long htime = System.nanoTime();
            int htotal = 0;
            int limit = 20000;
            try (Client eventClient = new Client("http://localhost:8000/", events)) {
              eventClient.open();
              int count = 0;
              Client.Result rsp = eventClient.scanForward(key1.array(), key2.array(), 100);
              count += rsp.count();
              keys.addAll(rsp.keys());
              while(rsp.token().length() > 0) {
                try {
                  rsp = eventClient.scanNext(100);
                  keys.addAll(rsp.keys());
                } catch(KdbException e) {
                  System.out.println(e);
                }
                count += rsp.count();
                if(count > limit)
                  break;
              }
              total += count;
              htotal += count;
            } catch(KdbException e) {
              System.out.println(e);
              continue;
            }
            double rdetime = (System.nanoTime()-htime)/1e9;
            //System.out.printf("query worker hcount %d  mcount %d read %d events in %e seconds avg %e\n", hcount, m5, htotal, rdetime, htotal/rdetime);
            process(stateClient, hcount, keys);
            keys.clear();
            double hetime = (System.nanoTime()-htime)/1e9;
            System.out.printf("query worker hcount %d  mcount %d counted %d events in %e seconds avg %e\n", hcount, m5, htotal, hetime, htotal/hetime);
            try {Thread.currentThread().sleep(rnd.nextInt(30000));} catch(Exception ex) {}
            stateClient.drop();
          } catch(KdbException e) {
            //System.out.println(e);
            System.out.printf("timeout hcount %d  mcount %d \n", hcount, m5);
            continue;
          }
          hcount++;
          System.out.printf("k %d query %d processed %d events\n", k, m5, total);
        }
      }
    }

  }

  public static class Compact implements Runnable {
    private int hour;

    public Compact(int hour) {
      this.hour = hour;
    }

    public void run() {
      Random rnd = new Random();
      while(true) {
        for (int m = 0; m < min5; m++) {
          try (Client client = new Client("http://localhost:8000/", events)) {
            client.open();
            long c1 = System.nanoTime();
            ByteBuffer k1 = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
            k1.put((byte)hour);
            k1.put((byte)m);
            ByteBuffer k2 = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
            k2.put((byte)hour);
            k2.put((byte)(m+1));
            //System.out.printf("starting compact h %d m %d \n", hour, m);
            client.compact(k1.array(), k2.array());
            long c2 = System.nanoTime();
            System.out.printf("compact h %d m %d took %e \n", hour, m, (c2-c1)/1e9);
            try {Thread.currentThread().sleep(rnd.nextInt(30000));} catch(Exception ex) {}
          } catch(KdbException e) {
            System.out.printf("compacting time out for h %d m = %d\n", hour, m);
          }
        }
      }
    }
  }

  public static void main(String[] args) {
    if(args.length < 3) {
      System.out.println("Program http://localhost:8000/ http://localhost:8001/ http://localhost:8002/");
      return;
    }

    uris = args;
    System.out.println("start");
    System.out.println("create events table");

    try (Client client = new Client("http://localhost:8000/", events)) {
      client.open("append", 300);
    }

    System.out.println("start event source threads");
    int num = 2;
    for (int i = 0; i < num; i++) {
      new Thread(new EventSource(i)).start();
    }

    try {Thread.currentThread().sleep(10000);} catch(Exception ex) {}

    System.out.println("start processing threads");
    for (int i = 0; i < min5; i++) {
      new Thread(new Query(i)).start();
    }

    System.out.println("start compaction worker");
    for (int i = 0; i < hours; i++) {
      new Thread(new Compact(i)).start();
    }

  }
}
