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

public class Guid2 {

  private static void process(Client client, List<byte[]> eventKeys) {
    int batchSize = 1000;
    List<byte[]> values = new ArrayList<byte[]>();
    List<byte[]> keys;
    int index = 0; int step = 100;
    int pcount = 0;
    int existedKeys  = 0;
    Random rnd = new Random();

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
  }

  static byte[] valueState;

  public static void main(String[] args) {
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    Random rnd = new Random();
    valueState = new byte[7000*8];
    rnd.nextBytes(valueState);

    for(int i = 0; i < 50000; i++) {
      UUID guid = UUID.randomUUID();
      ByteBuffer key = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
      key.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
      keys.add(key.array());
    }

    try (Client stateClient = new Client("http://localhost:8000/", "acme")) {
      stateClient.open();
      process(stateClient, keys);
    }
  }

}
