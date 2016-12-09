package kdb.rsm;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.BlockingQueue;
import kdb.rsm.proto.ZabMessage.Message;
import kdb.rsm.Zab.StateChangeCallback;
import kdb.rsm.Zab.FailureCaseCallback;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Participant state. It will be passed accross different instances of
 * Leader/Follower class during the lifetime of Zab.
 */
class ParticipantState {
  /**
   * Persistent variables for Zab.
   */
  private final PersistentState persistence;

  /**
   * The last delivered zxid. It's not persisted to disk. Just a place holder
   * to pass between Follower/Leader.
   */
  private Zxid lastDeliveredZxid = Zxid.ZXID_NOT_EXIST;

  /**
   * Message queue. The receiving callback simply parses the message and puts
   * it in queue, it's up to Leader/Follower/Election to take out
   * and process the message.
   */
  private final BlockingQueue<MessageTuple> messageQueue;

  /**
   * Used for communication between nodes.
   */
  private final Transport transport;

  /**
   * Server ID of the Participant.
   */
  private final String serverId;

  /**
   * The synchronization timeout in milliseconds. It will be adjusted
   * dynamically.
   */
  private int syncTimeoutMs;

  /**
   * The object for leader election.
   */
  private final Election election;

  /**
   * Callbacks for state change. Used for testing only.
   */
  private StateChangeCallback stateChangeCallback = null;

  /**
   * Callbacks for simulating failures. Used for testing only.
   */
  private FailureCaseCallback failureCallback = null;

  private static final Logger LOG =
    LogManager.getLogger(ParticipantState.class);

  ParticipantState(PersistentState persistence,
                   String serverId,
                   Transport transport,
                   BlockingQueue<MessageTuple> messageQueue,
                   StateChangeCallback stCallback,
                   FailureCaseCallback failureCallback,
                   int syncTimeoutMs,
                   Election election)
      throws IOException , GeneralSecurityException {
    this.persistence = persistence;
    this.serverId = serverId;
    this.messageQueue = messageQueue;
    this.stateChangeCallback = stCallback;
    this.failureCallback = failureCallback;
    this.transport = transport;
    this.syncTimeoutMs = syncTimeoutMs;
    this.election = election;
  }

  public BlockingQueue<MessageTuple> getMessageQueue() {
    return this.messageQueue;
  }

  public String getServerId() {
    return this.serverId;
  }

  public Transport getTransport() {
    return this.transport;
  }

  public PersistentState getPersistence() {
    return this.persistence;
  }

  public Zxid getLastDeliveredZxid() {
    return this.lastDeliveredZxid;
  }

  public int getSyncTimeoutMs() {
    return this.syncTimeoutMs;
  }

  public void setSyncTimeoutMs(int timeoutMs) {
    this.syncTimeoutMs = timeoutMs;
  }

  public void updateLastDeliveredZxid(Zxid zxid) {
    this.lastDeliveredZxid = zxid;
  }

  public StateChangeCallback getStateChangeCallback() {
    return this.stateChangeCallback;
  }

  public FailureCaseCallback getFailureCaseCallback() {
    return this.failureCallback;
  }

  public Election getElection() {
    return this.election;
  }

  public void enqueueShutdown() {
    Message shutdown = MessageBuilder.buildShutDown();
    this.messageQueue.add(new MessageTuple(this.serverId, shutdown));
  }

  public void enqueueMessage(MessageTuple tuple) {
    this.messageQueue.add(tuple);
  }
}
