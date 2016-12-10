import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import java.util.concurrent.Future;
import static java.util.stream.Collectors.*;
import java.util.stream.Collectors;
import com.google.gson.Gson;
import kdb.Client;

public class Subscriber {

  private static String primary = "xevents";
  private static String secondary = "xevents2-";
  private static List<String> evtsbuckets = new ArrayList<String>();

  public static class Options {
    String CompactionStyle;
    long MaxTableFilesSizeFIFO;
    int MaxBackgroundFlushes;
    int MaxBackgroundCompactions;
    int MaxWriteBufferNumber;
    int MinWriteBufferNumberToMerge;
  }

  private static String evtopts() {
    Options options = new Options();
    options.CompactionStyle = "FIFO";
    options.MaxTableFilesSizeFIFO = 1024*1024*1024*10L;
    options.MaxBackgroundFlushes = 2;
    options.MaxBackgroundCompactions = 4;
    options.MaxWriteBufferNumber = 32;
    options.MinWriteBufferNumberToMerge = 8;
    Gson gson = new Gson();
    return gson.toJson(options);
  }

  public static void main(String[] args) {
    int buckets = 6;

    for (int i = 0; i < buckets; i++) {
      try (Client client = new Client(args[0], secondary+i, evtopts())) {
        client.open("append");
      }
    }

    long lsn = 0;
    for (int i = 0; i < buckets; i++) {
      try(Client client = new Client(args[0], secondary+i)) {
        client.open();
        client.subscribe(args[1], primary+i, lsn);
      }
    }
    System.exit(0);
  }

}
