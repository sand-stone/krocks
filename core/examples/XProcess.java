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

public class XProcess {

  private static String events = "xevents";
  private static String states = "xstates";

  private static boolean stop;
  private static String[] uris;

  private static UUID[] deviceIds;

  private static int numT = 6;

  private static void init() {
    //deviceIds = new UUID[150000000];
    //deviceIds = new UUID[100000];
    deviceIds = new UUID[1000000];
    for(int i = 0; i < deviceIds.length; i++) {
      deviceIds[i] = UUID.randomUUID();
    }
  }

  public static class EventSource implements Runnable {
    private int id;
    private Random rnd;
    private int valSize;

    public EventSource(int id) {
      this.id  = id;
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
      try (Client client = new Client("http://localhost:8000/", events)) {
        client.open();
        int i = id;
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
            batchSize = keys.size();
            client.put(keys, values);
            keys.clear();
            values.clear();
            total += batchSize;
            long t2 = System.nanoTime();
            //System.out.printf("eventsource %d, bucket %d:%d batchSize %d total %d events takes %e seconds, rate %e \n",
            //                  id, i, j, batchSize, total,(t2-t1)/1e9, total/((t2-t1)/1e9));
          }
        }
      }
      System.out.printf("eventsource %d inserted %d events\n", id, total);
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

  public static class QueryState implements Runnable {
    private int id;
    private Random rnd;
    private int numquery;

    public QueryState(int id) {
      this.id = id;
      rnd = new Random();
      numquery = 7000;
    }

    private void deviceid(ByteBuffer buf) {
      UUID guid = deviceIds[rnd.nextInt(deviceIds.length)];
      buf.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
    }

    private int process(Client client, Client.Result result) {
      if(result.count() <= 0)
        return 0;
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      byte[] k = null; int joincount = 0;
      for(int j = 0; j < result.count(); j++) {
        if(k == null || memcmp(k, result.getKey(j), 18) != 0) {
          k = result.getKey(j);
          ByteBuffer key = ByteBuffer.allocate(18).order(ByteOrder.BIG_ENDIAN);
          key.put(result.getKey(j), 0, 18);
          keys.add(key.array());
          byte[] value = new byte[numquery*64];
          rnd.nextBytes(value);
          values.add(value);
        } else {
          joincount++;
        }
      }
      while(true) {
        try {
          client.put(keys, values);
          break;
        } catch(KdbException e) {
          System.out.println(e);
        }
      }
      return values.size();
    }

    public void run() {
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      long t1 = System.nanoTime();
      int b1 = id;
      try (Client client = new Client("http://localhost:8000/", events)) {
        client.open();
        int reprocessing = 100000;
        while(reprocessing-- > 0) {
          int count = 0;
          int scount = 0;
          System.out.println("##### reprocessing "+ reprocessing + " start process bucket:" + b1);
          for(int b2 = 0; b2 < numT; b2++) {
            ByteBuffer key1 = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
            key1.put((byte)b1).put((byte)b2);
            ByteBuffer key2 = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
            key2.put((byte)(b1)).put((byte)(b2+1));
            Client.Result rsp = client.scanForward(key1.array(), key2.array(), 1000);
            //System.out.println("rsp " + rsp);
            count += rsp.count();
            try(Client statesClient = new Client("http://localhost:8000/", states)) {
              scount += process(statesClient, rsp);
              while(rsp.token().length() > 0) {
                try {
                  rsp = client.scanNext(1000);
                } catch(KdbException e) {
                  System.out.println(e);
                }
                count += rsp.count();
                scount += process(statesClient, rsp);
              }
            }
            //System.out.println("##### reprocessing " + reprocessing + " done process bucket:" + b1 + " count: " + count + " scount: " + scount);
          }
          System.out.println("##### " + reprocessing + " " + count +  "scount: " + scount);
        }
      }
    }

  }

  public static void main(String[] args) {
    if(args.length < 3) {
      System.out.println("Program http://localhost:8000/ http://localhost:8001/ http://localhost:8002/");
      return;
    }

    init();

    uris = args;
    System.out.println("start");
    System.out.println("create table");

    try (Client client = new Client("http://localhost:8000/", events)) {
      client.open();
    }

    try (Client client = new Client("http://localhost:8000/", states)) {
      client.open();
    }

    int num = numT;
    for (int i = 0; i < num; i++) {
      new Thread(new EventSource(i)).start();
    }

    System.out.println("event source threads");

    try {Thread.currentThread().sleep(3000);} catch(Exception ex) {}

    for (int i = 0; i < num; i++) {
      new Thread(new QueryState(i)).start();
    }

    try {Thread.currentThread().sleep(120*60*1000);} catch(Exception ex) {}
    stop = true;
  }
}
