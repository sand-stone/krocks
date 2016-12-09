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

public class XProcess4 {

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
      int total = 0;
      int basem = LocalDateTime.now().getMinute() - 1;
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
    }
  }

  public static class Compact implements Runnable {
    private int hour;

    public Compact(int hour) {
      this.hour = hour;
    }

    public void run() {
      Random rnd = new Random();
      byte bucket = 0;
      while(true) {
        try (Client client = new Client("http://localhost:8000/", events)) {
          client.open();
          for(int i = 0; i<13; i++) {
            if(i == LocalDateTime.now().getMinute()/5)
              continue;
            long c1 = System.nanoTime();
            bucket = (byte)i;
            byte[] k1 = new byte[]{bucket};
            byte[] k2 = new byte[]{(byte)(bucket+1)};
            client.compact(k1, k2);
            long c2 = System.nanoTime();
            try {Thread.currentThread().sleep(rnd.nextInt(10000));} catch(Exception ex) {}
            System.out.printf("compact %d took %e \n", bucket, (c2-c1)/1e9);
          }
        } catch(KdbException e) {
          System.out.printf("compacting time out for bucket = %d\n", bucket);
        }
      }
    }

    public void run2() {
      Random rnd = new Random();
      byte bucket = 0;
      while(true) {
        try (Client client = new Client("http://localhost:8000/", events)) {
          client.open();
          long c1 = System.nanoTime();
          client.compact();
          long c2 = System.nanoTime();
          try {Thread.currentThread().sleep(30000);} catch(Exception ex) {}
          System.out.printf("compact %d took %e \n", bucket, (c2-c1)/1e9);
        } catch(KdbException e) {
          System.out.printf("compacting time out for bucket = %d\n", bucket);
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
      client.open("append", 60);
    }

    System.out.println("start event source threads");
    int num = 3;
    for (int i = 0; i < num; i++) {
      new Thread(new EventSource(i)).start();
    }

    try {Thread.currentThread().sleep(60000);} catch(Exception ex) {}

    System.out.println("start compaction worker");
    for (int i = 0; i < 1; i++) {
      new Thread(new Compact(i)).start();
    }
  }

}
