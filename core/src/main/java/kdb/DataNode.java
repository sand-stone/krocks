package kdb;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import kdb.proto.Database.*;
import kdb.rsm.ZabException;

final class DataNode {
  private static Logger log = LogManager.getLogger(DataNode.class);
  private Store store;
  private boolean standalone;
  private List<Ring> rings;
  private Random rnd;

  public DataNode(List<Ring> rings, boolean standalone) {
    this.rings = rings;
    this.store = Store.get();
    this.standalone = standalone;
    this.rnd = new Random();
  }

  public String stats(String table) {
    return store.stats(table);
  }

  private Ring ring() {
    return rings.get(rnd.nextInt(rings.size()));
  }

  private void rsend(Message msg, Object ctx) {
    try {
      ring().zab.send(ByteBuffer.wrap(msg.toByteArray()), ctx);
    } catch(ZabException.InvalidPhase e) {
      throw new KdbException(e);
    } catch(ZabException.TooManyPendingRequests e) {
      throw new KdbException(e);
    }
  }

  public void process(Message msg, Object context) {
    Message r = MessageBuilder.nullMsg;
    String table;
    //log.info("msg {} context {} standalone {}", msg, context, standalone);
    switch(msg.getType()) {
    case Open:
      table = msg.getOpenOp().getTable();
      if(standalone) {
        r = store.open(msg.getOpenOp());
      } else {
        rsend(msg, context);
      }
      break;
    case Compact:
      table = msg.getCompactOp().getTable();
      if(standalone) {
        r = store.compact(msg.getCompactOp());
      } else {
        rsend(msg, context);
      }
      break;
    case Drop:
      //log.info("msg {} context {}", msg, context);
      table = msg.getDropOp().getTable();
      if(standalone) {
        r = store.drop(msg.getDropOp());
      } else {
        rsend(msg, context);
      }
      break;
    case Get:
      r = store.get(msg.getGetOp());
      break;
    case Scan:
      r = store.scan(msg.getScanOp());
      break;
    case Sequence:
      r = store.seqno(msg.getSeqOp());
      break;
    case Scanlog:
      r = store.scanlog(msg.getScanlogOp());
      break;
    case Put:
      table = msg.getPutOp().getTable();
      r = store.update(msg.getPutOp());
      if(!standalone) {
        Message repl = MessageBuilder.buildSeqOp(table,
                                                 NettyTransport.get().dataaddr,
                                                 r.getResponse().getSeqno());
        rsend(repl, context);
      }
      break;
    default:
      r = MessageBuilder.emptyMsg;
      break;
    }
    if(r != MessageBuilder.nullMsg) {
      NettyTransport.HttpKdbServerHandler.reply(context, r);
    }
  }

}
