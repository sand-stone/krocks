import org.rocksdb.*;

public class WalkLog {

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

      while(true) {
        try (final TransactionLogIterator transactionLogIterator =
             db.getUpdatesSince(0)) {
          while(transactionLogIterator.isValid()) {
            /*final TransactionLogIterator.BatchResult batchResult =
              transactionLogIterator.getBatch();
            try(WriteBatch wb = batchResult.writeBatch()) {
            }*/
            transactionLogIterator.next();
          }
        }
      }
    }
  }
}
