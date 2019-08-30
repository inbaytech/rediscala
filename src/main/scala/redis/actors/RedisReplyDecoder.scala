package redis.actors

import java.io.IOException

import akka.actor.{Actor, ActorRef, Props}

import scala.collection.mutable
import redis.protocol.{DecodeResult, FullyDecoded, RedisProtocolReply, RedisReply}
import akka.util.ByteString
import akka.event.Logging

import scala.annotation.tailrec
import redis.{Operation, RedisServerConfig}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

case object Tick
case object Timeout

class RedisReplyDecoder(xmitter: ActorRef, config: RedisServerConfig) extends Actor {

  val queuePromises = mutable.Queue[Operation[_,_]]()

  val log = Logging(context.system, this)

  val timer = config.responseTimeout.map(t => context.system.scheduler.schedule(t, t, self, Tick))

  private def onTick() = {
    queuePromises.headOption.foreach { op =>
      if (System.nanoTime - op.timestamp > config.responseTimeout.get.toNanos) {
        xmitter ! Timeout
      }
    }
  }

  override def postStop(): Unit = {
    queuePromises.foreach(op => {
      op.completeFailed(InvalidRedisReply)
    })
  }

  def receive = {
    case promises: QueuePromises => {
      queuePromises ++= promises.queue
    }
    case byteStringInput: ByteString => decodeReplies(byteStringInput)
    case Tick => onTick()
  }

  var partiallyDecoded: DecodeResult[Unit] = DecodeResult.unit

  def decodeReplies(dataByteString: ByteString): Unit = {
    partiallyDecoded = if (partiallyDecoded.isFullyDecoded) {
      decodeRepliesRecur(partiallyDecoded.rest ++ dataByteString)
    } else {
      val r = partiallyDecoded.run(dataByteString)
      if (r.isFullyDecoded) {
        decodeRepliesRecur(r.rest)
      } else {
        r
      }
    }
  }

  @tailrec
  private def decodeRepliesRecur(bs: ByteString): DecodeResult[Unit] = {
    if (queuePromises.nonEmpty && bs.nonEmpty) {
      val op = queuePromises.dequeue()
      val result = decodeRedisReply(op, bs)

      if (result.isFullyDecoded) {
        decodeRepliesRecur(result.rest)
      } else {
        result
      }
    } else {
      FullyDecoded((), bs)
    }
  }

  def decodeRedisReply(operation: Operation[_, _], bs: ByteString): DecodeResult[Unit] = {
    if (operation.redisCommand.decodeRedisReply.isDefinedAt(bs)) {
      operation.decodeRedisReplyThenComplete(bs)
    } else if (RedisProtocolReply.decodeReplyError.isDefinedAt(bs)) {
      RedisProtocolReply.decodeReplyError.apply(bs)
        .foreach { error =>
          operation.completeFailed(ReplyErrorException(error.toString))
        }
    } else {
      operation.completeFailed(InvalidRedisReply)
      throw new Exception(s"Redis Protocol error: Got ${bs.head} as initial reply byte for Operation: $operation")
    }
  }
}

case class ReplyErrorException(message: String) extends Exception(message)

object InvalidRedisReply extends RuntimeException("Could not decode the redis reply (Connection closed)")

trait DecodeReplies {
  var partiallyDecoded: DecodeResult[Unit] = DecodeResult.unit

  def decodeReplies(dataByteString: ByteString): Unit = {
    partiallyDecoded = if (partiallyDecoded.isFullyDecoded) {
      decodeRepliesRecur(dataByteString)
    } else {
      val r = partiallyDecoded.run(dataByteString)
      if (r.isFullyDecoded) {
        decodeRepliesRecur(r.rest)
      } else {
        r
      }
    }
  }

  @tailrec
  private def decodeRepliesRecur(bs: ByteString): DecodeResult[Unit] = {
    val r = RedisProtocolReply.decodeReply(bs).map(onDecodedReply)
    if (r.isFullyDecoded) {
      decodeRepliesRecur(r.rest)
    } else {
      r
    }
  }

  def onDecodedReply(reply: RedisReply): Unit
}

object RedisReplyDecoder {
  def props(xmitter: ActorRef, config: RedisServerConfig): Props = Props(new RedisReplyDecoder(xmitter, config))
}

case class QueuePromises(queue: mutable.Queue[Operation[_, _]])
