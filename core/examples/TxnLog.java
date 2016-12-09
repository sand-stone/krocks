import org.rocksdb.*;

public class TxnLog {

  private static class BatchHandler extends WriteBatch.Handler {

    public BatchHandler() {
    }

    public void put(byte[] key, byte[] value) {
      System.out.printf("put key %s value %s\n", new String(key), new String(value));
    }

    public void merge(byte[] key, byte[] value) {
      System.out.printf("merge key %s value %s", key.toString(), value.toString());
    }

    public void delete(byte[] key) {
      System.out.printf("delete key %s value %s", key.toString());
    }

    public void logData(byte[] blob) {
      System.out.printf("log key %s value %s", blob.toString());
    }
  }


  public static void main(String[] args) throws Exception {
    final int numberOfPuts = 5;
    try (final Options options = new Options()
         .setCreateIfMissing(true)
         .setWalTtlSeconds(1000)
         .setWalSizeLimitMB(10);
         final RocksDB db = RocksDB.open(options, "acme")) {

      for (int i = 0; i < numberOfPuts; i++) {
        db.put(String.valueOf(i).getBytes(),
               String.valueOf(i).getBytes());
      }
      db.flush(new FlushOptions().setWaitForFlush(true));

      System.out.println("lsn:" + db.getLatestSequenceNumber());

      // insert 5 writes into a cf
      try (final ColumnFamilyHandle cfHandle = db.createColumnFamily(
                                                                     new ColumnFamilyDescriptor("new_cf".getBytes()))) {
        for (int i = 0; i < numberOfPuts; i++) {
          db.put(cfHandle, String.valueOf(i).getBytes(),
                 String.valueOf(i).getBytes());
        }

        System.out.println("lsn:" + db.getLatestSequenceNumber());
        for (int i = 0; i < numberOfPuts; i++) {
          db.put(String.valueOf(i).getBytes(),
                 String.valueOf(i).getBytes());
        }
        db.flush(new FlushOptions().setWaitForFlush(true));

        System.out.println("lsn:" + db.getLatestSequenceNumber());

        // Get updates since the beginning
        try (final TransactionLogIterator transactionLogIterator =
             db.getUpdatesSince(0)) {
          while(transactionLogIterator.isValid()) {
            transactionLogIterator.status();
            // The first sequence number is 1
            final TransactionLogIterator.BatchResult batchResult =
              transactionLogIterator.getBatch();
            //System.out.println("batch lsn:" + batchResult.sequenceNumber());
            try {
              WriteBatch wb = batchResult.writeBatch();
              System.out.println("wb class " + wb.getClass());
              System.out.println("wb count " + wb.count());
              wb.iterate(new BatchHandler());
            } catch(RocksDBException e) {
              System.out.println("e:" + e.getMessage());
            }
            transactionLogIterator.next();
          }
        }
      }
    }
  }
}
