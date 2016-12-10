package kdb;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.toList;
import com.google.protobuf.ByteString;
import java.util.*;
import kdb.proto.Database.*;
import kdb.proto.Database.Message.MessageType;

final class MessageBuilder {

  final static Message nullMsg = MessageBuilder.buildErrorResponse("null");
  final static Message emptyMsg = MessageBuilder.buildResponse("empty");

  private final static List<String> emptyList = new ArrayList<String>();

  public static Message buildResponse(String msg) {
    Response op = Response
      .newBuilder()
      .setType(Response.Type.OK)
      .setReason(msg)
      .build();
    return Message.newBuilder().setType(MessageType.Response).setResponse(op).build();
  }

  public static Message buildErrorResponse(String error) {
    Response op = Response
      .newBuilder()
      .setType(Response.Type.Error)
      .setReason(error)
      .build();
    return Message.newBuilder().setType(MessageType.Response).setResponse(op).build();
  }

  public static Message buildOpenOp(String table, List<String> columns, String merge, int ttl, String compression, String options, String backupOptions) {
    OpenOperation op = OpenOperation
      .newBuilder()
      .setTable(table)
      .setTtl(ttl)
      .setCompression(compression == null? "": compression)
      .setMergeOperator(merge == null? "" : merge)
      .addAllColumns(columns == null? emptyList : columns)
      .setOptions(options == null? "" : options)
      .setBackupOptions(backupOptions == null? "" : backupOptions)
      .build();
    return Message.newBuilder().setType(MessageType.Open).setOpenOp(op).build();
  }

  public static Message buildPutOp(String table, String column, List<byte[]> keys, List<byte[]> values) {
    PutOperation op = PutOperation
      .newBuilder()
      .setTable(table)
      .setColumn(column == null? "" : column)
      .addAllKeys(keys.stream().map(k -> ByteString.copyFrom(k)).collect(toList()))
      .addAllValues(values.stream().map(v -> ByteString.copyFrom(v)).collect(toList()))
      .build();
    return Message.newBuilder().setType(MessageType.Put).setPutOp(op).build();
  }

  public static Message buildGetOp(String table, String column, List<byte[]> keys) {
    GetOperation op = GetOperation
      .newBuilder()
      .setTable(table)
      .setColumn(column == null? "" : column)
      .addAllKeys(keys.stream().map(k -> ByteString.copyFrom(k)).collect(toList()))
      .build();
    return Message.newBuilder().setType(MessageType.Get).setGetOp(op).build();
  }

  public static Message buildScanOp(ScanOperation.Type op, String table, String column, String token, int limit, byte[] key, byte[] key2) {
    ScanOperation scan = ScanOperation
      .newBuilder()
      .setOp(op)
      .setTable(table)
      .setColumn(column == null? "" : column)
      .setToken(token == null? "" : token)
      .setLimit(limit)
      .setKey(key == null? ByteString.EMPTY : ByteString.copyFrom(key))
      .setKey2(key2 == null? ByteString.EMPTY : ByteString.copyFrom(key2))
      .build();
    return Message.newBuilder().setType(MessageType.Scan).setScanOp(scan).build();
  }

  public static Message buildSubcribeOp(String table, String uri, String source, long seqno) {
    SubscribeOperation subscribe = SubscribeOperation
      .newBuilder()
      .setTable(table)
      .setUri(uri)
      .setSource(source)
      .setSeqno(seqno)
      .build();
    return Message.newBuilder().setType(MessageType.Subscribe).setSubscribeOp(subscribe).build();
  }

  public static Message buildResponse(String token, List<byte[]> keys, List<byte[]> values) {
    Response op = Response
      .newBuilder()
      .setType(Response.Type.OK)
      .setToken(token)
      .addAllKeys(keys
               .stream()
               .map(k -> ByteString.copyFrom(k))
               .collect(toList()))
      .addAllValues(values
               .stream()
               .map(v -> ByteString.copyFrom(v))
               .collect(toList()))
      .build();
    return Message.newBuilder().setType(MessageType.Response).setResponse(op).build();
  }

  public static Message buildSeq(long seqno) {
    Response op = Response
      .newBuilder()
      .setType(Response.Type.OK)
      .setSeqno(seqno)
      .build();
    return Message.newBuilder().setType(MessageType.Response).setResponse(op).build();
  }

  public static Message buildLog(long seqno, byte[] logops, List<byte[]> keys, List<byte[]> values) {
    Response op = Response
      .newBuilder()
      .setType(Response.Type.OK)
      .setSeqno(seqno)
      .setLogops(ByteString.copyFrom(logops))
      .addAllKeys(keys
                  .stream()
                  .map(k -> ByteString.copyFrom(k))
               .collect(toList()))
      .addAllValues(values
                    .stream()
                    .map(v -> ByteString.copyFrom(v))
                    .collect(toList()))
      .build();
    return Message.newBuilder().setType(MessageType.Response).setResponse(op).build();
  }

  public static Message buildResponse(Map<byte[], byte[]> data) {
    if(data == null)
      return emptyMsg;
    Response op = Response
      .newBuilder()
      .setType(Response.Type.OK)
      .addAllKeys(data
               .keySet()
               .stream()
               .map(k -> ByteString.copyFrom(k))
               .collect(toList()))
      .addAllValues(data
               .values()
               .stream()
               .map(v -> ByteString.copyFrom(v))
               .collect(toList()))
      .build();
    return Message.newBuilder().setType(MessageType.Response).setResponse(op).build();
  }

  public static Message buildDropOp(String table) {
    DropOperation op = DropOperation
      .newBuilder()
      .setTable(table)
      .build();
    return Message.newBuilder().setType(MessageType.Drop).setDropOp(op).build();
  }

  public static Message buildDropOp(String table, String column) {
    DropOperation op = DropOperation
      .newBuilder()
      .setTable(table)
      .setColumn(column)
      .build();
    return Message.newBuilder().setType(MessageType.Drop).setDropOp(op).build();
  }

  public static Message buildCompactOp(String table, String column, byte[] begin, byte[] end) {
    CompactOperation op = CompactOperation
      .newBuilder()
      .setTable(table)
      .setColumn(column == null? "" : column)
      .setBegin(begin == null? ByteString.EMPTY : ByteString.copyFrom(begin))
      .setEnd(end == null? ByteString.EMPTY : ByteString.copyFrom(end))
      .build();
    return Message.newBuilder().setType(MessageType.Compact).setCompactOp(op).build();
  }

  public static Message buildSeqOp(String table) {
    SequenceOperation op = SequenceOperation
      .newBuilder()
      .setTable(table)
      .build();
    return Message.newBuilder().setType(MessageType.Sequence).setSeqOp(op).build();
  }

  public static Message buildScanlogOp(String table, long seqno, int limit) {
    ScanlogOperation op = ScanlogOperation
      .newBuilder()
      .setTable(table)
      .setSeqno(seqno)
      .setLimit(limit)
      .build();
    return Message.newBuilder().setType(MessageType.Scanlog).setScanlogOp(op).build();
  }

}
