package ktsdb;

import java.util.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import com.google.common.collect.ImmutableList;
import kdb.Client;
import com.google.gson.Gson;

/*

| field  | type                     |
|--------|--------------------------|
| metric | string                   |
| tagset | map\<string, string\>    |
| time   | timestamp (µs precision) |
| value  | double                   |

metrics table
 [key: <metric string, tagset_id int32, time long> value: <value double>]

tagsets table
  [ key: <id int32> value: <tagset blob> ]

tags table
  [key: <key string, value string, tageset_id int32>, value: <>]

 */

class Tables {
  private Tables() {}

  static List<String> metricsBuckets = new ArrayList<String>();

  static String metricsTableName(String tsName) {
    return String.format("ktsdb.%s.metrics", tsName);
  }

  static String tagsetsTableName(String tsName) {
    return String.format("ktsdb.%s.tagsets", tsName);
  }

  static String tagsTableName(String tsName) {
    return String.format("ktsdb.%s.tags", tsName);
  }

  public static byte[] toBytes(int num) {
    ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(num);
    return buf.array();
  }

  public static int toInt(byte[] data) {
    ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
    return buf.getInt();
  }

  public static byte[] toBytes(double num) {
    ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
    buf.putDouble(num);
    return buf.array();
  }

  public static double toDouble(byte[] data) {
    ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
    return buf.getDouble();
  }

  public static byte[] toBytes(long num) {
    ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
    buf.putLong(num);
    return buf.array();
  }

  public static long toLong(byte[] data) {
    ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
    return buf.getLong();
  }

  public static class Options {
    String CompactionStyle;
    long MaxTableFilesSizeFIFO;
    int MaxBackgroundFlushes;
    int MaxBackgroundCompactions;
    int MaxWriteBufferNumber;
    int MinWriteBufferNumberToMerge;
  }

  private static String metricsOpts() {
    Options options = new Options();
    options.CompactionStyle = "FIFO";
    options.MaxTableFilesSizeFIFO = 1024*1024*1024*5L;
    options.MaxBackgroundFlushes = 2;
    options.MaxBackgroundCompactions = 4;
    options.MaxWriteBufferNumber = 32;
    options.MinWriteBufferNumberToMerge = 8;
    Gson gson = new Gson();
    return gson.toJson(options);
  }

  private static String tagsOpts() {
    Options options = new Options();
    options.CompactionStyle = "FIFO";
    options.MaxTableFilesSizeFIFO = 1024*1024*1024*60L;
    options.MaxBackgroundFlushes = 2;
    options.MaxBackgroundCompactions = 4;
    options.MaxWriteBufferNumber = 32;
    options.MinWriteBufferNumberToMerge = 8;
    Gson gson = new Gson();
    return gson.toJson(options);
  }

  private static String tagsetsOpts() {
    Options options = new Options();
    options.CompactionStyle = "FIFO";
    options.MaxTableFilesSizeFIFO = 1024*1024*1024*60L;
    options.MaxBackgroundFlushes = 2;
    options.MaxBackgroundCompactions = 4;
    options.MaxWriteBufferNumber = 32;
    options.MinWriteBufferNumberToMerge = 8;
    Gson gson = new Gson();
    return gson.toJson(options);
  }

  static void createTables(String uri, String tsName) {
    try (Client client = new Client(uri,
                                    metricsTableName(tsName),
                                    metricsOpts())) {
      for(int i = 0; i < 13; i++) {
        metricsBuckets.add("bucket"+i);
      }
      client.open(metricsBuckets, "append");
    }

    try (Client client = new Client(uri, tagsTableName(tsName), tagsOpts())) {
      client.openCompressed("snappy");
    }

    try (Client client = new Client(uri, tagsetsTableName(tsName), tagsetsOpts())) {
      client.openCompressed("snappy");
    }

  }
}
