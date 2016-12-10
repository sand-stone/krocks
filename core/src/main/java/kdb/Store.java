package kdb;

import java.nio.*;
import java.io.*;
import java.lang.reflect.Array;
import com.google.gson.*;
import java.util.*;
import java.util.stream.*;
import static java.util.stream.Collectors.toList;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.rocksdb.*;
import kdb.proto.Database.*;
import kdb.proto.Database.Message.MessageType;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class Store implements Closeable {
  private static Logger log = LogManager.getLogger(Store.class);
  private static Store instance;

  private String db;
  private String location;
  private ConcurrentHashMap<String, DataTable> tables;
  private Timer timer;
  private Gson gson;

  static {
    RocksDB.loadLibrary();
    instance = new Store();
  }

  static class Cursor {
    public RocksIterator cursor;
    public byte[] marker;

    public Cursor(RocksIterator cursor, byte[] marker) {
      this.cursor = cursor;
      this.marker = marker;
    }

    public void close() {
      cursor.close();
    }
  }

  static class DataTable {
    public RocksDB db;
    public List<ColumnFamilyDescriptor> colDs;
    public LinkedHashMap<String, ColumnFamilyHandle> columns;
    public String merge;
    public ConcurrentHashMap<String, Cursor> cursors;
    public Statistics stats;
    public AtomicInteger counts;

    public DataTable() {
      db = null;
      columns = null;
      merge = null;
      colDs = null;
      cursors = new ConcurrentHashMap<String, Cursor>();
      stats = null;
      counts = new AtomicInteger();
    }

    public ColumnFamilyHandle getCol(String col) {
      if(col.length() == 0)
        return columns.get("default");
      ColumnFamilyHandle handle = columns.get(col);
      if(handle == null) {
        throw new KdbException("col does not exist" + col);
      }
      return handle;
    }

    public void inc() {
      counts.getAndIncrement();
    }

    public void dec() {
      counts.getAndDecrement();
    }

    public boolean active() {
      return counts.get() > 0;
    }

    public void close() {
      cursors.values().stream().forEach(c -> c.close());
      if(columns != null) {
        columns.values().stream().forEach(h -> h.close());
      }
      db.close();
    }
  }

  public static Store get() {
    return instance;
  }

  public void bind(String location) {
    Utils.mkdir(location);
    this.location = location;
  }

  public Store() {
    tables = new ConcurrentHashMap<String, DataTable>();
    timer = new Timer();
    gson = new Gson();
  }

  private static TimerTask wrap(Runnable r) {
    return new TimerTask() {
      @Override
      public void run() {
        r.run();
      }
    };
  }

  private static class KdbBackupOptions extends HashMap<String, Object> {};

  private BackupableDBOptions parseBackupOptions(String json) {
    KdbBackupOptions opts = gson.fromJson(json, KdbBackupOptions.class);
    BackupableDBOptions backupOptions;
    if(opts != null) {
      String path = (String)opts.get("Path");
      Random rnd = new Random();
      log.info("back path {}", path);
      Utils.mkdir(path);
      backupOptions = new BackupableDBOptions(path);
      opts.forEach((name, v)->{
          switch(name) {
          case "ShareTableFiles":
            backupOptions.setShareTableFiles((boolean)v);
            break;
          case "Sync":
            backupOptions.setSync((boolean)v);
            break;
          case "DestroyOldData":
            backupOptions.setDestroyOldData((boolean)v);
            break;
          case "BackupLogFiles":
            backupOptions.setBackupLogFiles((boolean)v);
            break;
          case "BackupRateLimit":
            backupOptions.setBackupRateLimit((long)((double)v));
            break;
          case "RestoreRateLimit":
            backupOptions.setRestoreRateLimit((long)((double)v));
            break;
          case "ShareFilesWithChecksum":
            backupOptions.setShareFilesWithChecksum((boolean)v);
            break;
          }
        });
    } else {
      backupOptions = new BackupableDBOptions("default-backup");
    }
    return backupOptions;
  }

  private RocksDB getDB(Options options, String backupOptions, String path, int ttl) throws RocksDBException {
    //log.info("options {}", backupOptions);
    if(ttl == -1) {
      if(backupOptions.length() == 0)
        return RocksDB.open(options, path);
      else {
        return BackupableDB.open(options, parseBackupOptions(backupOptions), path);
      }
    }
    //log.info("create {} ttl {}", path, ttl);
    return TtlDB.open(options, path, ttl, false);
  }

  private RocksDB getDB(final DBOptions options, final String path,
                        final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
                        final List<ColumnFamilyHandle> columnFamilyHandles,
                        int ttl) throws RocksDBException {

    if(ttl == -1) {
      return RocksDB.open(options, path, columnFamilyDescriptors, columnFamilyHandles);
    } else {
      List<Integer> ttls = new ArrayList<Integer>();
      for(int i = 0; i < columnFamilyHandles.size(); i++) {
        ttls.add(ttl);
      }
      return TtlDB.open(options, path, columnFamilyDescriptors, columnFamilyHandles, ttls, false);
    }
  }

  private CompressionType getCompression(String name) {
    CompressionType ct = CompressionType.NO_COMPRESSION;
    switch(name) {
    case "snappy":
      ct = CompressionType.SNAPPY_COMPRESSION;
      break;
    case "lz4":
      ct = CompressionType.LZ4_COMPRESSION;
      break;
    case "z":
      ct = CompressionType.ZLIB_COMPRESSION;
      break;
    case "lz4hc":
      ct = CompressionType.LZ4HC_COMPRESSION;
      break;
    case "bzip2":
      ct = CompressionType.BZLIB2_COMPRESSION;
      break;
    }
    return ct;
  }

  private static class KdbOptions extends HashMap<String, String> {};

  private int toInt(String v) {
    try {
      return Integer.parseInt(v);
    } catch(NumberFormatException e) {}
    return 0;
  }

  private long toLong(String v) {
    try {
      return Long.parseLong(v);
    } catch(NumberFormatException e) {}
    return 0;
  }

  private void parseOptions(Options options, String json) {
    KdbOptions opts = gson.fromJson(json, KdbOptions.class);
    if(opts != null) {
      opts.forEach((name, v)->{
          switch(name) {
          case "CompactionStyle":
            switch(v) {
            case "FIFO":
              options.setCompactionStyle(CompactionStyle.FIFO);
              break;
            case "LEVEL":
              options.setCompactionStyle(CompactionStyle.LEVEL);
              break;
            default:
              options.setCompactionStyle(CompactionStyle.UNIVERSAL);
              break;
            }
            break;
          case "MaxTableFilesSizeFIFO":
            options.setMaxTableFilesSizeFIFO(toLong(v));
            break;
          case "MaxBackgroundFlushes":
            options.setMaxBackgroundFlushes(toInt(v));
            break;
          case "MaxBackgroundCompactions":
            options.setMaxBackgroundCompactions(toInt(v));
            break;
          case "MaxWriteBufferNumber":
            options.setMaxWriteBufferNumber(toInt(v));
            break;
          case "MinWriteBufferNumberToMerge":
            options.setMinWriteBufferNumberToMerge(toInt(v));
            break;
          case "NumLevels":
            options.setNumLevels(toInt(v));
            break;
          case "MaxBytesForLevelBase":
            options.setMaxBytesForLevelBase(toLong(v));
            break;
          case "MaxBytesForLevelMultiplier":
            options.setMaxBytesForLevelMultiplier(toInt(v));
            break;
          case "LevelZeroFileNumCompactionTrigger":
            options.setLevelZeroFileNumCompactionTrigger(toInt(v));
            break;
          case "LevelZeroSlowdownWritesTrigger":
            options.setLevelZeroSlowdownWritesTrigger(toInt(v));
            break;
          case "LevelZeroStopWritesTrigger":
            options.setLevelZeroStopWritesTrigger(toInt(v));
            break;
          case "WalTtlSeconds":
            options.setWalTtlSeconds(toInt(v));
            break;
          }
        });
    }
  }

  private void setMergeOperator(Options options, String mergeOperator) {
    //log.info("{} merge: <{}>", op, mergeOperator);
    if(mergeOperator != null && mergeOperator.length() > 0) {
      switch(mergeOperator) {
      case "add":
        options.setMergeOperatorName("uint64add");
        break;
      case "append":
        options.setMergeOperatorName("stringappend");
        break;
      case"max":
        options.setMergeOperatorName("max");
        break;
      default:
        throw new KdbException("wrong merge operator, valid ones: add, append, max");
      }
    }
  }

  private void setMergeOperator(ColumnFamilyOptions options, String mergeOperator) {
    //log.info("column merge: <{}>", mergeOperator);
    if(mergeOperator != null && mergeOperator.length() > 0) {
      switch(mergeOperator) {
      case "add":
        options.setMergeOperatorName("uint64add");
        break;
      case "append":
        options.setMergeOperatorName("stringappend");
        break;
      case"max":
        options.setMergeOperatorName("max");
        break;
      default:
        throw new KdbException("wrong merge operator, valid ones: add, append, max");
      }
    }
  }

  private void parseOptions(ColumnFamilyOptions options, String json) {
    KdbOptions opts = gson.fromJson(json, KdbOptions.class);
    if(opts != null) {
      opts.forEach((name, v)->{
          switch(name) {
          case "CompactionStyle":
            switch(v) {
            case "FIFO":
              options.setCompactionStyle(CompactionStyle.FIFO);
              break;
            case "LEVEL":
              options.setCompactionStyle(CompactionStyle.LEVEL);
              break;
            default:
              options.setCompactionStyle(CompactionStyle.UNIVERSAL);
              break;
            }
            break;
          case "MaxTableFilesSizeFIFO":
            options.setMaxTableFilesSizeFIFO(toLong(v));
            break;
          case "MaxWriteBufferNumber":
            options.setMaxWriteBufferNumber(toInt(v));
            break;
          case "MinWriteBufferNumberToMerge":
            options.setMinWriteBufferNumberToMerge(toInt(v));
            break;
          case "NumLevels":
            options.setNumLevels(toInt(v));
            break;
          case "MaxBytesForLevelBase":
            options.setMaxBytesForLevelBase(toLong(v));
            break;
          case "MaxBytesForLevelMultiplier":
            options.setMaxBytesForLevelMultiplier(toInt(v));
            break;
          case "LevelZeroFileNumCompactionTrigger":
            options.setLevelZeroFileNumCompactionTrigger(toInt(v));
            break;
          case "LevelZeroSlowdownWritesTrigger":
            options.setLevelZeroSlowdownWritesTrigger(toInt(v));
            break;
          case "LevelZeroStopWritesTrigger":
            options.setLevelZeroStopWritesTrigger(toInt(v));
            break;
          }
        });
    }
  }

  private void appendStats(RocksDB db, ColumnFamilyHandle handle, StringBuilder builder) {
    try {
      builder.append(db.getProperty(handle, "rocksdb.stats"));
    }  catch(RocksDBException e) {
    }
  }

  private void report(StringBuilder builder, String name, DataTable dt) {
    builder.append("\n\n\t\t\ttable " + name + "\n");
    try {
      if(dt.columns != null) {
        dt.columns.values().stream().forEach(h -> appendStats(dt.db, h, builder));
      } else {
        builder.append(dt.db.getProperty("rocksdb.stats"));
      }
    } catch(RocksDBException e) {
      builder.append("no rocksdb.stats\n");
    }
    /*builder.append("tickers\n");
      for (TickerType statsType : TickerType.values()) {
      builder.append(gson.toJson(dt.stats.getTickerCount(statsType)));
      builder.append("\t");
      }
      builder.append("histograms\n");
      for (HistogramType histogramType : HistogramType.values()) {
      builder.append(gson.toJson(dt.stats.getHistogramData(histogramType)));
      builder.append("\n");
      }*/
  }

  public String stats(String table) {
    StringBuilder builder = new StringBuilder();
    builder.append("\n\t\t\t\t Kdb Stats \n");
    Gson gson = new Gson();
    if(table.length() == 0) {
      tables.forEach((name, dt)-> report(builder, name, dt));
    } else {
      DataTable dt = tables.get(table);
      if(dt != null) {
        report(builder, table, dt);
      }
    }
    return builder.toString();
  }

  public synchronized Message open(OpenOperation op) {
    String table = op.getTable();
    if(table == null || table.length() == 0)
      return MessageBuilder.buildResponse("table name needed");

    if(tables.get(table) == null) {
      String path = location+"/"+table;
      Utils.mkdir(path);
      DataTable dt = new DataTable();
      String mergeOperator = op.getMergeOperator();
      int ttl = op.getTtl();
      //log.info("create {} ttl {}", table, ttl);
      try(Options options = new Options().setCreateIfMissing(true)) {
        options.createStatistics();
        //log.info("options <{}>", op.getOptions());
        options.setCompressionType(getCompression(op.getCompression()));
        options.setAllowConcurrentMemtableWrite(true);
        options.setEnableWriteThreadAdaptiveYield(true);
        options.setCompactionStyle(CompactionStyle.UNIVERSAL);
        options.setIncreaseParallelism(Runtime.getRuntime().availableProcessors());
        parseOptions(options, op.getOptions());
        dt.stats = options.statisticsPtr();
        dt.merge = mergeOperator.length() == 0? null : mergeOperator;
        RocksDB db = null;
        try {
          List<String> columns = op.getColumnsList();
          if(columns.size() == 0) {
            setMergeOperator(options, dt.merge);
            db = getDB(options, op.getBackupOptions(), path, ttl);
          } else {
            try(final RocksDB db2 = getDB(options, op.getBackupOptions(), path, ttl)) {
              assert(db2 != null);
              columns.stream().forEach(col -> {
                  try(ColumnFamilyOptions colOpts = new ColumnFamilyOptions()) {
                    parseOptions(colOpts, op.getOptions());
                    setMergeOperator(colOpts, dt.merge);
                    try(ColumnFamilyHandle columnFamilyHandle = db2
                        .createColumnFamily(
                                            new ColumnFamilyDescriptor(col.getBytes(), colOpts))) {
                    } catch (RocksDBException e) {
                      throw new KdbException(e);
                    }}
                });
            } catch (RocksDBException e) {
              log.info(e);
              return MessageBuilder.buildResponse("open: " + e.getMessage());
            } catch (KdbException e) {
              log.info(e);
              return MessageBuilder.buildResponse("open: " + e.getMessage());
            }
            dt.colDs = new ArrayList<>();
            List<ColumnFamilyHandle> handles = new ArrayList<>();
            dt.colDs
              .add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
                                              new ColumnFamilyOptions()));
            columns.stream().forEach(col -> {
                ColumnFamilyOptions colOpts = new ColumnFamilyOptions();
                parseOptions(colOpts, op.getOptions());
                setMergeOperator(colOpts, dt.merge);
                dt.colDs
                  .add(new ColumnFamilyDescriptor(col.getBytes(),
                                                  colOpts));
              });
            try(DBOptions dboptions = new DBOptions()) {
              parseOptions(options, op.getOptions());
              db = getDB(dboptions, path, dt.colDs, handles, ttl);
            }
            dt.columns = new LinkedHashMap<String, ColumnFamilyHandle>();
            //log.info("handles {} columns {}", handles.size(), columns);
            dt.columns.put("default", handles.get(0));
            for(int i = 0; i < columns.size(); i++) {
              dt.columns.put(columns.get(i), handles.get(i+1));
            }
          }
        } catch (RocksDBException e) {
          log.info(e);
          return MessageBuilder.buildResponse(e.getMessage());
        } catch (KdbException e) {
          log.info(e);
          return MessageBuilder.buildResponse(e.getMessage());
        }
        dt.db = db;
        tables.putIfAbsent(table, dt);
      }
    }
    return MessageBuilder.buildResponse("open " + table);
  }

  public Message compact(CompactOperation op) {
    //log.info("compact {}", op);
    DataTable table = tables.get(op.getTable());
    if(table == null)
      return MessageBuilder.buildErrorResponse("table name needed");

    if(op.getColumn() != null && op.getColumn().length() > 0) {
      ColumnFamilyHandle handle = table.columns.get(op.getColumn());
      if(handle == null) {
        return MessageBuilder.buildErrorResponse("column does not exist");
      }
      if(op.getBegin().size() == 0 || op.getEnd().size() == 0) {
        try {
          table.db.compactRange(handle);
        } catch(RocksDBException e) {
          log.info(e);
        }
      } else {
        try {
          table.db.compactRange(handle, op.getBegin().toByteArray(), op.getEnd().toByteArray());
        } catch(RocksDBException e) {
          log.info(e);
        }
      }
    } else {
      if(op.getBegin().size() == 0 || op.getEnd().size() == 0) {
        try {
          table.db.compactRange();
        } catch(RocksDBException e) {
          log.info(e);
        }
      } else {
        try {
          table.db.compactRange(op.getBegin().toByteArray(), op.getEnd().toByteArray());
        } catch(RocksDBException e) {
          log.info(e);
        }
      }
    }
    return MessageBuilder.buildResponse("compact " + table);
  }

  public Message drop(DropOperation op) {
    //log.info("drop {}", op);
    String table = op.getTable();
    if(table == null || table.length() == 0)
      return MessageBuilder.buildErrorResponse("table name needed");

    String path = location+"/"+table;
    if(tables.get(table) == null) {
      if(!Utils.checkFile(path))
        return MessageBuilder.buildErrorResponse("table does not exist:" + table);
      Utils.deleteFile(path);
      return MessageBuilder.buildResponse("drop " + table);
    }

    DataTable dt = tables.get(table);
    if(dt.active()) {
      log.info("try to drop active table {}", table);
      return MessageBuilder.buildErrorResponse("table still active:" + table);
    }
    String col = op.getColumn();
    if(col != null && col.length() > 0) {
      try {
        //log.info("drop col {}", col);
        dt.db.dropColumnFamily(dt.getCol(col));
      } catch(RocksDBException e) {
        log.info(e);
        return MessageBuilder.buildResponse("cannot drop " + col);
      } catch(KdbException e) {
        log.info(e);
        return MessageBuilder.buildResponse("cannot drop " + col);
      }
      return MessageBuilder.buildResponse("drop " + col);
    }

    tables.remove(table).close();
    //log.info("delete {}", path);
    Utils.deleteFile(path);
    return MessageBuilder.buildResponse("drop " + table);
  }

  public void update(String table, Client.Result rsp) {
    DataTable dt = tables.get(table);
    if(dt != null) {
      try(WriteOptions writeOpts = new WriteOptions();
          WriteBatch writeBatch = new WriteBatch()) {
        byte[] ops = rsp.logops();
        int vc = 0;
        for(int i = 0; i < ops.length; i++) {
          switch(ops[i]) {
          case 0:
            writeBatch.put(rsp.getKey(i), rsp.getValue(vc++));
            break;
          case 1:
            writeBatch.merge(rsp.getKey(i), rsp.getValue(vc++));
            break;
          default:
            break;
          }
        }
        try {
          dt.inc();
          dt.db.write(writeOpts, writeBatch);
        } finally {
          dt.dec();
        }
      } catch(RocksDBException e) {
        e.printStackTrace();
        log.info(e);
      }
    }
  }

  public Message update(PutOperation op) {
    String name = op.getTable();
    DataTable table = tables.get(name);
    if(table == null) {
      return MessageBuilder.buildErrorResponse("table not opened:" + table);
    }

    int len = op.getKeysCount();
    if(len != op.getValuesCount()) {
      return MessageBuilder.buildErrorResponse("data length wrong");
    }
    try {
      table.inc();
      if(op.getColumn() != null && op.getColumn().length() > 0) {
        //log.info("op.getColumn() {}", op.getColumn());
        ColumnFamilyHandle handle = table.columns.get(op.getColumn());
        if(handle == null) {
          return MessageBuilder.buildErrorResponse("column does not exist");
        }
        if(table.merge == null) {
          try(WriteOptions writeOpts = new WriteOptions();
              WriteBatch writeBatch = new WriteBatch()) {
            for(int i = 0; i < len; i++) {
              writeBatch.put(handle, op.getKeys(i).toByteArray(), op.getValues(i).toByteArray());
            }
            table.db.write(writeOpts, writeBatch);
          } catch (RocksDBException e) {
            e.printStackTrace();
            log.info(e);
            return MessageBuilder.buildErrorResponse("updated wrong" + e.getMessage());
          }
        } else {
          //log.info("merge {} op.getColumn() {}", table.merge, op.getColumn());
          try(WriteOptions writeOpts = new WriteOptions();
              WriteBatch writeBatch = new WriteBatch()) {
            for(int i = 0; i < len; i++) {
              writeBatch.merge(handle, op.getKeys(i).toByteArray(), op.getValues(i).toByteArray());
            }
            table.db.write(writeOpts, writeBatch);
          } catch (RocksDBException e) {
            e.printStackTrace();
            log.info(e);
            return MessageBuilder.buildErrorResponse("updated wrong" + e.getMessage());
          }
        }
      } else {
        if(table.merge == null) {
          try(WriteOptions writeOpts = new WriteOptions();
              WriteBatch writeBatch = new WriteBatch()) {
            for(int i = 0; i < len; i++) {
              writeBatch.put(op.getKeys(i).toByteArray(), op.getValues(i).toByteArray());
            }
            table.db.write(writeOpts, writeBatch);
          } catch (RocksDBException e) {
            e.printStackTrace();
            log.info(e);
            return MessageBuilder.buildErrorResponse("updated wrong" + e.getMessage());
          }
        } else {
          try(WriteOptions writeOpts = new WriteOptions();
              WriteBatch writeBatch = new WriteBatch()) {
            for(int i = 0; i < len; i++) {
              writeBatch.merge(op.getKeys(i).toByteArray(), op.getValues(i).toByteArray());
            }
            table.db.write(writeOpts, writeBatch);
          } catch (RocksDBException e) {
            e.printStackTrace();
            log.info(e);
            return MessageBuilder.buildErrorResponse("updated wrong" + e.getMessage());
          }
        }
      }
    } finally {
      table.dec();
    }
    return MessageBuilder.buildResponse("updated " + table);
  }

  private ReadOptions getReadOptions() {
    return getReadOptions(false);
  }

  private ReadOptions getReadOptions(boolean prefix) {
    ReadOptions readOptions = new ReadOptions();
    readOptions.setTotalOrderSeek(true);
    if(prefix)
      readOptions.setPrefixSameAsStart(prefix);
    return readOptions;
  }

  private void walk(Cursor cursor, ScanOperation.Type dir, int limit, List<byte[]> keys, List<byte[]> values) {
    int count = 0;
    byte[] marker = cursor.marker;
    if(marker == null) {
      switch(dir) {
      case Next:
        while(cursor.cursor.isValid()) {
          byte[] key = cursor.cursor.key();
          byte[] value = cursor.cursor.value();
          keys.add(Arrays.copyOf(key, key.length));
          values.add(Arrays.copyOf(value, value.length));
          cursor.cursor.next();
          if(++count >= limit)
            return;
        }
        break;
      case Prev:
        while(cursor.cursor.isValid()) {
          byte[] key = cursor.cursor.key();
          byte[] value = cursor.cursor.value();
          keys.add(Arrays.copyOf(key, key.length));
          values.add(Arrays.copyOf(value, value.length));
          cursor.cursor.prev();
          if(++count >= limit)
            return;
        }
        break;
      }
    } else {
      switch(dir) {
      case Next:
        //log.info("marker {} limit {} count {} ", new String(marker), limit, count);
        while(cursor.cursor.isValid()) {
          byte[] key = cursor.cursor.key();
          byte[] value = cursor.cursor.value();
          if(marker.length > 0) {
            if(Utils.memcmp(key, marker, marker.length) < 0) {
              keys.add(Arrays.copyOf(key, key.length));
              values.add(Arrays.copyOf(value, value.length));
              if(++count >= limit) {
                cursor.cursor.next();
                return;
              }
            } else {
              cursor.cursor.next();
              break;
            }
          } else {
            keys.add(Arrays.copyOf(key, key.length));
            values.add(Arrays.copyOf(value, value.length));
            if(++count >= limit) {
              cursor.cursor.next();
              return;
            }
          }
          cursor.cursor.next();
        }
        //log.info("seek next {}", count);
        break;
      case Prev:
        while(cursor.cursor.isValid()) {
          byte[] key = cursor.cursor.key();
          byte[] value = cursor.cursor.value();
          if(marker.length > 0) {
            if(Utils.memcmp(key, marker, marker.length) > 0) {
              keys.add(Arrays.copyOf(key, key.length));
              values.add(Arrays.copyOf(value, value.length));
              if(++count >= limit) {
                cursor.cursor.prev();
                return;
              }
            } else {
              cursor.cursor.prev();
              break;
            }
          } else {
            keys.add(Arrays.copyOf(key, key.length));
            values.add(Arrays.copyOf(value, value.length));
            if(++count >= limit) {
              cursor.cursor.prev();
              return;
            }
          }
          cursor.cursor.prev();
        }
        break;
      }
    }
  }

  public Message get(GetOperation op) {
    String name = op.getTable();
    DataTable table = tables.get(name);
    try {
      table.inc();
      if(table != null) {
        if(op.getKeysCount() > 1000) {
          //review: random guess
          return MessageBuilder.buildErrorResponse("batch size too big");
        }
        String col = op.getColumn();
        if(col != null && col.length() > 0) {
          try {
            int count = op.getKeysCount();
            List<ColumnFamilyHandle> handles = new ArrayList<ColumnFamilyHandle>(count);
            ColumnFamilyHandle handle = table.getCol(col);
            if(handle == null)
              return MessageBuilder.buildErrorResponse("wrong column:" + col);
            for(int i = 0; i < count; i++)
              handles.add(handle);
            //log.info("col <{}> handle {}", col, handle);
            return MessageBuilder.buildResponse(table
                                                .db
                                                .multiGet(handles,
                                                          op
                                                          .getKeysList()
                                                          .stream()
                                                          .map(k -> k.toByteArray())
                                                          .collect(toList())));
          } catch(RocksDBException e) {
            log.info(e);
            return MessageBuilder.buildErrorResponse("table get errr:" + e.getMessage());
          }
        } else {
          try {
            return MessageBuilder.buildResponse(table
                                                .db
                                                .multiGet(op
                                                          .getKeysList()
                                                          .stream()
                                                          .map(k -> k.toByteArray())
                                                          .collect(toList())));
          } catch(RocksDBException e) {
            log.info(e);
            return MessageBuilder.buildErrorResponse("table get errr:" + e.getMessage());
          }
        }
      }
    } finally {
      table.dec();
    }
    return MessageBuilder.buildErrorResponse("table not opened:" + table);
  }

  private  boolean isempty(String col) {
    return col == null || col.length() == 0;
  }

  public Message scan(ScanOperation op) {
    //log.info("scan {}", op);
    String name = op.getTable();
    DataTable table = tables.get(name);
    if(table == null) {
      return MessageBuilder.buildErrorResponse("table not opened:" + table);
    }
    String token = "";
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    try {
      table.inc();
      RocksIterator iter = null;
      Cursor cursor = null;
      String col = op.getColumn();
      int limit = op.getLimit();
      switch(op.getOp()) {
      case First:
        if(isempty(col))
          iter = table.db.newIterator(getReadOptions());
        else
          iter = table.db.newIterator(table.getCol(col), getReadOptions());
        iter.seekToFirst();
        cursor = new Cursor(iter, null);
        token = cursor.toString();
        table.cursors.put(token, cursor);
        walk(cursor, ScanOperation.Type.Next, limit, keys, values);
        break;
      case Last:
        if(isempty(col))
          iter = table.db.newIterator(getReadOptions());
        else
          iter = table.db.newIterator(table.getCol(col), getReadOptions());
        iter.seekToLast();
        cursor = new Cursor(iter, null);
        token = cursor.toString();
        table.cursors.put(token, cursor);
        walk(cursor, ScanOperation.Type.Prev, limit, keys, values);
        break;
      case Close:
        token = op.getToken();
        cursor = table.cursors.remove(token);
        //log.info("close token <{}> ==> {}", token, cursor);
        if(cursor != null) {
          cursor.close();
          token = "";
        }
        break;
      case ScanNext:
        //log.info("scan col: {}", col);
        if(isempty(col))
          iter = table.db.newIterator(getReadOptions());
        else
          iter = table.db.newIterator(table.getCol(col), getReadOptions());
        iter.seek(op.getKey().toByteArray());
        cursor = new Cursor(iter, op.getKey2().toByteArray());
        token = cursor.toString();
        table.cursors.put(token, cursor);
        walk(cursor, ScanOperation.Type.Next, limit, keys, values);
        break;
      case ScanPrev:
        if(isempty(col))
          iter = table.db.newIterator(getReadOptions());
        else
          iter = table.db.newIterator(table.getCol(col), getReadOptions());
        iter.seek(op.getKey().toByteArray());
        cursor = new Cursor(iter, op.getKey2().toByteArray());
        token = cursor.toString();
        table.cursors.put(token, cursor);
        walk(cursor, ScanOperation.Type.Prev, limit, keys, values);
        break;
      case Next:
        token = op.getToken();
        cursor = table.cursors.get(token);
        if(cursor != null) {
          walk(cursor, ScanOperation.Type.Next, limit, keys, values);
          if(keys.size() == 0)
            token = "";
        }
        break;
      case Prev:
        token = op.getToken();
        cursor = table.cursors.get(token);
        if(cursor != null) {
          walk(cursor, ScanOperation.Type.Prev, limit, keys, values);
          if(keys.size() == 0)
            token = "";
        }
        break;
      }
    } finally {
      table.dec();
    }
    return  MessageBuilder.buildResponse(token, keys, values);
  }

  public Message seqno(SequenceOperation op) {
    String name = op.getTable();
    DataTable table = tables.get(name);
    if(table == null) {
      return MessageBuilder.buildErrorResponse("table not opened:" + table);
    }
    try {
      table.inc();
      return MessageBuilder.buildSeq(table.db.getLatestSequenceNumber());
    } finally {
      table.dec();
    }
  }

  private static class BatchHandler extends WriteBatch.Handler {
    public List<byte[]> keys;
    public List<byte[]> values;
    public List<Byte> ops;

    public BatchHandler(List<Byte> ops, List<byte[]> keys, List<byte[]> values) {
      this.keys = keys;
      this.values = values;
      this.ops = ops;
    }

    public void put(byte[] key, byte[] value) {
      keys.add(key);
      values.add(value);
      ops.add((byte)0);
    }

    public void merge(byte[] key, byte[] value) {
      //log.info("merge key {} value {}", new String(key), new String(value));
      keys.add(key);
      values.add(value);
      ops.add((byte)1);
    }

    public void delete(byte[] key) {
      keys.add(key);
      ops.add((byte)2);
    }

    public void logData(byte[] blob) {
      keys.add(blob);
      ops.add((byte)3);
    }
  }

  private void process(WriteBatch batch, List<Byte> ops, List<byte[]> keys, List<byte[]> values) throws RocksDBException {
    try(BatchHandler handler = new BatchHandler(ops, keys, values)) {
      batch.iterate(handler);
    } catch (RocksDBException e) {
      //review: ignore column family updates
    }
  }

  public Message scanlog(ScanlogOperation op) {
    String name = op.getTable();
    DataTable dt = tables.get(name);
    if(dt == null) {
      return MessageBuilder.buildErrorResponse("table not opened:" + name);
    }
    Message ret = MessageBuilder.emptyMsg;
    int limit = op.getLimit();
    try(TransactionLogIterator iter = dt.db.getUpdatesSince(op.getSeqno())) {
      dt.inc();
      //log.info("last seq {}", op.getSeqno());
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      List<Byte> ops = new ArrayList<Byte>();
      int count = 0;
      long seqno = 0;
      while(iter.isValid()) {
        iter.status();
        TransactionLogIterator.BatchResult batch = iter.getBatch();
        seqno = batch.sequenceNumber();
        try(WriteBatch wb = batch.writeBatch()) {
          process(wb, ops, keys, values);
        }
        if(++count >= limit)
          break;
        iter.next();
      }
      byte[] logops = new byte[ops.size()];
      for(int i = 0; i < ops.size(); i++) {
        logops[i] = ops.get(i);
      }
      ret = MessageBuilder.buildLog(seqno, logops, keys, values);
    } catch (RocksDBException e) {
      log.info("scan log {}", e);
    } finally {
      dt.dec();
    }
    return ret;
  }

  public Message handle(ByteBuffer data) throws IOException {
    byte[] arr = new byte[data.remaining()];
    data.get(arr);
    Message msg = Message.parseFrom(arr);
    //log.info("handle {}", msg);
    if(msg.getType() == MessageType.Put) {
      PutOperation op = msg.getPutOp();
      msg = update(op);
    } else if(msg.getType() == MessageType.Open) {
      OpenOperation op = msg.getOpenOp();
      msg = open(op);
    } else if(msg.getType() == MessageType.Drop) {
      msg = drop(msg.getDropOp());
    } else if(msg.getType() == MessageType.Compact) {
      msg = compact(msg.getCompactOp());
    }
    return msg;
  }

  public void close() { }

}
