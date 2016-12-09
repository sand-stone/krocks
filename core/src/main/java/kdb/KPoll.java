package kdb;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.concurrent.ConcurrentHashMap;
import kdb.proto.Database.*;

class KPoll  {
  private static Logger log = LogManager.getLogger(KPoll.class);

  private ThreadPoolExecutor executor;
  private ConcurrentHashMap<String, Poller> pollers;

  private static KPoll instance = new KPoll();

  public static KPoll get() {
    return instance;
  }

  KPoll() {
    executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*10);
    pollers = new ConcurrentHashMap<String, Poller>();
  }

  private String getKey(String uri, String table) {
    return uri + "/" + table;
  }

  public Message process(SubscribeOperation op) {
    Message r;
    String key = getKey(op.getUri(), op.getSource());
    if(op.getSeqno() == -1) {
      //log.info("unsub op {}", op);
      Poller poller = pollers.remove(key);
      if(poller != null)
        poller.stop();
      r = MessageBuilder.buildResponse("unsubscribe " + op.getUri() + "/" + op.getSource());
    } else {
      Poller poller = new Poller(op);
      Poller prev = pollers.putIfAbsent(key, poller);
      if(prev != null)
        prev.stop();
      schedule(new Poller(op));
      r = MessageBuilder.buildResponse("subscribe " + key);
    }
    return r;
  }

  private void schedule(Runnable task) {
    executor.execute(task);
  }

  public static class Poller implements Runnable {
    SubscribeOperation op;
    private boolean stop;

    public Poller(SubscribeOperation op) {
      this.op = op;
      stop = false;
    }

    public void stop() {
      stop = true;
    }

    public void run() {
      long seqno = op.getSeqno();
      log.info("poller start {}", op);
      try (Client client = new Client(op.getUri(), op.getSource())) {
        client.open();
        while(!stop) {
          int limit = 1000;
          try {
            do {
              if(stop || seqno >= client.getLatestSequenceNumber())
                break;
              Client.Result rsp = client.scanlog(seqno,limit);
              if(seqno == rsp.seqno())
                break;
              //log.info("poller got log {} == {} == {}", seqno, rsp.seqno(), client.getLatestSequenceNumber());
              Store.get().update(op.getTable(), rsp);
              seqno = rsp.seqno();
            } while(!stop);
            log.info(" {} catches up {}.{}", op.getTable(), op.getUri(), op.getSource());
          } catch(KdbException e) {
            log.info(e);
          }
          try {Thread.currentThread().sleep(1000);} catch(InterruptedException ex) {}
        }
      }
      log.info("poller {} stopped", op);
    }
  }
}
