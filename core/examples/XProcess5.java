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
import java.time.LocalDateTime;
import com.google.gson.Gson;

public class XProcess5 {

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
      deviceIds = new UUID[125000]; //(150000000/3600.0)*300/100
      for(int i = 0; i < deviceIds.length; i++) {
        deviceIds[i] = UUID.randomUUID();
      }
    }

    private void write(Client client, List<byte[]> keys, List<byte[]> values) {
      int retry = 5;
      do {
        try {
          client.put(keys, values);
          return;
        } catch(KdbException e) {
          retry--;
        }
      } while(retry>0);
      throw new KdbException("timed out");
    }

    public void run() {
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      int batchSize = 1000;
      long total = 0;
      int basem = LocalDateTime.now().getMinute() - 1;
      while(true) {
        int hour = LocalDateTime.now().getHour();
        try (Client client = new Client("http://localhost:8000/", events)) {
          client.open();
          int m = 0;
          do {
            LocalDateTime now = LocalDateTime.now();
            m = now.getMinute();
            byte bucket = (byte)(m/5);
            keys.clear();
            values.clear();
            init();
            long t1 = System.nanoTime();
            for(int i = 0; i < deviceIds.length; i++) {
              UUID guid = deviceIds[i];
              for(int k = 0; k < 6; k++) {
                ByteBuffer key = ByteBuffer.allocate(17).order(ByteOrder.BIG_ENDIAN);
                key.put(bucket);
                key.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
                keys.add(key.array());
                byte[] value = new byte[valSize];
                rnd.nextBytes(value);
                values.add(value);
              }
              if(keys.size() >= batchSize) {
                write(client, keys, values);
                total += keys.size();
                keys.clear();
                values.clear();
              }
            }
            if(keys.size() > 0) {
              write(client, keys, values);
              total += keys.size();
            }
            long t2 = System.nanoTime();
            System.out.printf("source %d total %d for bucket %d took %e \n", id, total, bucket, (t2-t1)/1e9);
          } while (m != basem);
        } catch(KdbException e) {
          System.out.printf("event source %d failed", id);
        }
        System.out.printf("source %d finished running for hour %d \n", id, hour);
      }
    }
  }

  public static class Query implements Runnable {
    private int id;
    private byte[] valueState;
    private Random rnd;

    public Query(int id) {
      this.id = id;
      rnd = new Random();
      valueState = new byte[7000*8];
      rnd.nextBytes(valueState);
    }

    private void getEvents(byte[] key1, byte[] key2, List<byte[]> keys) {
      try (Client eventClient = new Client("http://localhost:8000/", events)) {
        eventClient.open();
        int count = 0;
        Client.Result rsp = eventClient.scanForward(key1, key2, 1000);
        count += rsp.count();
        keys.addAll(rsp.keys());
        while(rsp.token().length() > 0) {
          try {
            rsp = eventClient.scanNext(1000);
            keys.addAll(rsp.keys());
            count += rsp.count();
          } catch(KdbException e) {
            System.out.println(e);
          }
        }
        if(count > 0) {
          processEvents(keys);
        }
      }
    }

    private void processEvents(List<byte[]> eventKeys) {
      if(eventKeys.size() == 0)
        return;
      try (Client client = new Client("http://localhost:8000/", states+id)) {
        long s1 = System.nanoTime();
        client.openCompressed("snappy");
        List<byte[]> values = new ArrayList<byte[]>();
        List<byte[]> keys = new ArrayList<byte[]>();
        int index = 0; int step = 100;
        int count = 0;
        int existedKeys  = 0;
        for(index = 0; (index + step) < eventKeys.size(); index += step) {
          keys.add(eventKeys.get(index));
          //Client.Result rsp = client.get(keys);
          values.add(Arrays.copyOf(valueState, valueState.length));
          if(keys.size() > 1000) {
            client.put(keys, values);
            count += keys.size();
            keys.clear();
            values.clear();
          }
        }
        long s2 = System.nanoTime();
        System.out.printf("q %d state writing keys %d in %e seconds \n", id, count, (s2-s1)/1e9);
        client.drop();
      }
    }

    public void run() {
      List<byte[]> keys = new ArrayList<byte[]>();

      while(true) {
        for (int i = 0; i < 13; i++) {
          byte bucket = (byte)i;
          byte[] k1 = new byte[]{bucket};
          byte[] k2 = new byte[]{(byte)(bucket+1)};
          keys.clear();
          try {
            long t1 = System.nanoTime();
            getEvents(k1, k2, keys);
            long t2 = System.nanoTime();
            System.out.printf("q %d for bucket %d read %d keys in %e seconds \n", id, bucket, keys.size(), (t2-t1)/1e9);
            t1 = System.nanoTime();
            processEvents(keys);
            t2 = System.nanoTime();
            System.out.printf("q %d for bucket %d processed %d keys in %e seconds \n", id, bucket, keys.size(), (t2-t1)/1e9);
          } catch(Exception e) {
            System.out.printf(" processor %d get %s \n", id, e.getMessage());
          }
        }
      }
    }
  }

  public static class Options {
    String CompactionStyle;
    long MaxTableFilesSizeFIFO;
    int MaxBackgroundFlushes;
    int MaxBackgroundCompactions;
    int MaxWriteBufferNumber;
    int MinWriteBufferNumberToMerge;
  }

  private static String opts() {
    Options options = new Options();
    options.CompactionStyle = "FIFO";
    options.MaxTableFilesSizeFIFO = 1024*1024*1024*20L;
    options.MaxBackgroundFlushes = 2;
    options.MaxBackgroundCompactions = 4;
    options.MaxWriteBufferNumber = 8;
    options.MinWriteBufferNumberToMerge = 4;
    Gson gson = new Gson();
    return gson.toJson(options);
  }

  public static void main(String[] args) {
    if(args.length < 3) {
      System.out.println("Program http://localhost:8000/ http://localhost:8001/ http://localhost:8002/");
      return;
    }

    uris = args;
    System.out.println("start");
    System.out.println("create events table");

    try (Client client = new Client("http://localhost:8000/", events, opts())) {
      client.open("append", 30*60);
    }

    System.out.println("start event source threads");
    int num = 3;
    for (int i = 0; i < num; i++) {
      new Thread(new EventSource(i)).start();
    }

    System.out.println("start query worker");
    for (int i = 0; i < 5; i++) {
      new Thread(new Query(i)).start();
    }
  }

}
