package redis.actors

import java.net.InetSocketAddress

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.util.{ByteString, ByteStringBuilder}
import redis.{Operation, RedisServer, RedisServerConfig, Transaction}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

object RedisClientActor {

  def props(
    server: RedisServer,
    config: RedisServerConfig,
    getConnectOperations: () => Seq[Operation[_, _]],
    onConnectStatus: Boolean => Unit,
    dispatcherName: String
    ) =
    Props(new RedisClientActor(server, config, getConnectOperations, onConnectStatus, dispatcherName))
}

class RedisClientActor(server: RedisServer, config: RedisServerConfig, getConnectOperations: () =>
  Seq[Operation[_, _]], onConnectStatus: Boolean => Unit, dispatcherName: String, connectTimeout: Option[FiniteDuration] = None) extends
  RedisWorkerIO(new InetSocketAddress(server.host, server.port), onConnectStatus, config.connectTimeout) {

  import context._

  var repliesDecoder = initRepliesDecoder()

  // connection closed on the sending direction
  var oldRepliesDecoder: Option[ActorRef] = None

  def initRepliesDecoder() = context.actorOf(RedisReplyDecoder.props(self, config).withDispatcher(dispatcherName))

  var queuePromises = mutable.Queue[Operation[_, _]]()

  def writing: Receive = {
    case op: Operation[_, _] =>
      queuePromises enqueue op
      write(op.redisCommand.encodedRequest)
    case Transaction(commands) => {
      val buffer = new ByteStringBuilder
      commands.foreach(operation => {
        buffer.append(operation.redisCommand.encodedRequest)
        queuePromises enqueue operation
      })
      write(buffer.result())
    }
    case Terminated(actorRef) =>
      log.warning(s"Terminated($actorRef)")
    case KillOldRepliesDecoder => killOldRepliesDecoder()
    case Timeout => abort()
  }

  def onDataReceived(dataByteString: ByteString): Unit = {
    repliesDecoder ! dataByteString
  }

  def onDataReceivedOnClosingConnection(dataByteString: ByteString): Unit = {
    oldRepliesDecoder.foreach(oldRepliesDecoder => oldRepliesDecoder ! dataByteString)
  }

  def onWriteSent(): Unit = {
    repliesDecoder ! QueuePromises(queuePromises)
    queuePromises = mutable.Queue[Operation[_, _]]()
  }

  def onConnectionClosed(): Unit = {
    queuePromises.foreach(op => {
      op.completeFailed(NoConnectionException)
    })
    queuePromises.clear()
    killOldRepliesDecoder()
    oldRepliesDecoder = Some(repliesDecoder)
    // TODO send delayed message to oldRepliesDecoder to kill himself after X seconds
    this.context.system.scheduler.scheduleOnce(reconnectDuration * 10, self, KillOldRepliesDecoder)
    repliesDecoder = initRepliesDecoder()
  }

  def onClosingConnectionClosed(): Unit = killOldRepliesDecoder()

  def killOldRepliesDecoder() = {
    oldRepliesDecoder.foreach(oldRepliesDecoder => oldRepliesDecoder ! PoisonPill)
    oldRepliesDecoder = None
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case _: Exception => {
        // Start a new decoder
        repliesDecoder = initRepliesDecoder()
        restartConnection()
        // stop the old one => clean the mailbox
        Stop
      }
    }

  def onConnectWrite(): ByteString = {
    val ops = getConnectOperations()
    val buffer = new ByteStringBuilder

    val queuePromisesConnect = mutable.Queue[Operation[_, _]]()
    ops.foreach(operation => {
      buffer.append(operation.redisCommand.encodedRequest)
      queuePromisesConnect enqueue operation
    })
    queuePromises = queuePromisesConnect ++ queuePromises
    buffer.result()
  }

}

case object NoConnectionException extends RuntimeException("No Connection established")

case object KillOldRepliesDecoder
