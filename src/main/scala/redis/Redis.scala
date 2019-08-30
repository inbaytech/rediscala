package redis

import akka.actor._
import akka.util.Helpers
import redis.commands._

import scala.concurrent._
import java.net.InetSocketAddress

import redis.actors.{RedisClientActor, RedisSubscriberActorWithCallback}
import redis.api.pubsub._
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration.FiniteDuration

trait RedisCommands
  extends Keys
  with Strings
  with Hashes
  with Lists
  with Sets
  with SortedSets
  with Publish
  with Scripting
  with Connection
  with Server
  with HyperLogLog
  with Clusters
  with Geo

case class RedisTopology(
  redis   : Either[RedisServer, Seq[RedisServer]]
) {
  def server = redis.left.get
  def nodes = redis.right.get
}

object RedisTopology {
  def apply(host: String, port: Int): RedisTopology = RedisTopology(Left(RedisServer(host, port, None, None)))
  def apply(server: RedisServer): RedisTopology = RedisTopology(Left(server))
  def apply(servers: Seq[RedisServer]): RedisTopology = RedisTopology(Right(servers))
}

case class RedisServerConfig(
  connectTimeout  : Option[FiniteDuration],
  responseTimeout : Option[FiniteDuration]
) {
}

object RedisServerConfig {
  def default: RedisServerConfig = RedisServerConfig(None, None)
  def apply(connectTimeout: FiniteDuration, responseTimeout: FiniteDuration): RedisServerConfig = RedisServerConfig(Some(connectTimeout), Some(responseTimeout))
}

case class RedisConfiguration(
  topology: RedisTopology,
  config: RedisServerConfig
) {
}

object RedisConfiguration {
  def apply(host: String, port: Int): RedisConfiguration = RedisConfiguration(RedisTopology(host, port), RedisServerConfig(None, None))
  def apply(server: RedisServer): RedisConfiguration = RedisConfiguration(RedisTopology(server), RedisServerConfig(None, None))
  def apply(servers: Seq[RedisServer]): RedisConfiguration = RedisConfiguration(RedisTopology(servers), RedisServerConfig(None, None))
}

abstract class RedisClientActorLike(server: RedisServer, config: RedisServerConfig, name: String, system: ActorSystem, redisDispatcher: RedisDispatcher) extends ActorRequest {
  var host = server.host
  var port = server.port
  val password: Option[String] = None
  val db: Option[Int] = None
  implicit val executionContext = system.dispatchers.lookup(redisDispatcher.name)

  val redisConnection: ActorRef = system.actorOf(RedisClientActor.props(server, config,
    getConnectOperations, onConnectStatus, redisDispatcher.name)
      .withDispatcher(redisDispatcher.name),
    name + '-' + Redis.tempName()
  )

  def reconnect(host: String = host, port: Int = port) = {
    if (this.host != host || this.port != port ) {
      this.host = host
      this.port = port
      redisConnection ! new InetSocketAddress(host, port)
    }
  }

  def onConnect(redis: RedisCommands): Unit = {
    password.foreach(redis.auth(_)) // TODO log on auth failure
    db.foreach(redis.select(_))
  }

  def onConnectStatus: (Boolean) => Unit = (status: Boolean) => {

  }

  def getConnectOperations: () => Seq[Operation[_, _]] = () => {
    val self = this
    val redis = new BufferedRequest with RedisCommands {
      implicit val executionContext: ExecutionContext = self.executionContext
    }
    onConnect(redis)
    redis.operations.result()
  }

  /**
   * Disconnect from the server (stop the actor)
   */
  def stop(): Unit = {
    system stop redisConnection
  }
}

case class RedisClient(server: RedisServer, config: RedisServerConfig, name: String = "RedisClient")
                      (implicit _system: ActorSystem,
                       redisDispatcher: RedisDispatcher = Redis.dispatcher
                      ) extends RedisClientActorLike(server, config, name, _system,redisDispatcher) with RedisCommands with Transactions {

}

object RedisClient {
  def apply()(implicit _system: ActorSystem): RedisClient = RedisClient(
    RedisServer("localhost", 6379), RedisServerConfig.default
  )

  def apply(server: RedisServer)(implicit _system: ActorSystem): RedisClient = RedisClient(
    server, RedisServerConfig.default
  )

  def apply(config: RedisServerConfig)(implicit _system: ActorSystem): RedisClient = RedisClient(
    RedisServer("localhost", 6379), config
  )
}

case class RedisBlockingClient(server: RedisServer, config: RedisServerConfig, name: String)
                              (implicit _system: ActorSystem,
                               redisDispatcher: RedisDispatcher = Redis.dispatcher
                              ) extends RedisClientActorLike(server, config, name, _system, redisDispatcher) with BLists {
}

object RedisBlockingClient {
  def apply()(implicit _system: ActorSystem): RedisBlockingClient = {
    RedisBlockingClient(RedisServer("localhost", 6379), RedisServerConfig.default, "RedisBlockingClient")
  }
  def apply(config: RedisServerConfig)(implicit _system: ActorSystem): RedisBlockingClient = {
    RedisBlockingClient(RedisServer("localhost", 6379), config, "RedisBlockingClient")
  }
}

case class RedisPubSub(
                        host: String = "localhost",
                        port: Int = 6379,
                        channels: Seq[String],
                        patterns: Seq[String],
                        onMessage: Message => Unit = _ => {},
                        onPMessage: PMessage => Unit = _ => {},
                        authPassword: Option[String] = None,
                        name: String = "RedisPubSub"
                        )(implicit system: ActorRefFactory,
                          redisDispatcher: RedisDispatcher = Redis.dispatcher) {

  val redisConnection: ActorRef = system.actorOf(
    Props(classOf[RedisSubscriberActorWithCallback],
      new InetSocketAddress(host, port), channels, patterns, onMessage, onPMessage, authPassword,onConnectStatus)
      .withDispatcher(redisDispatcher.name),
    name + '-' + Redis.tempName()
  )

  /**
   * Disconnect from the server (stop the actor)
   */
  def stop(): Unit = {
    system stop redisConnection
  }

  def subscribe(channels: String*): Unit = {
    redisConnection ! SUBSCRIBE(channels: _*)
  }

  def unsubscribe(channels: String*): Unit = {
    redisConnection ! UNSUBSCRIBE(channels: _*)
  }

  def psubscribe(patterns: String*): Unit = {
    redisConnection ! PSUBSCRIBE(patterns: _*)
  }

  def punsubscribe(patterns: String*): Unit = {
    redisConnection ! PUNSUBSCRIBE(patterns: _*)
  }

  def onConnectStatus(): (Boolean) => Unit = (status: Boolean) => {

  }
}

case class SentinelMonitoredRedisClient( config: RedisConfiguration,
                                         master: String,
                                         password: Option[String] = None,
                                         db: Option[Int] = None,
                                         name: String = "SMRedisClient")
                                       (implicit system: ActorSystem,
                                        redisDispatcher: RedisDispatcher = Redis.dispatcher
                                        ) extends SentinelMonitoredRedisClientLike(config, system, redisDispatcher) with RedisCommands with Transactions {

  val redisClient: RedisClient = withMasterAddr((ip, port) => {
    new RedisClient(RedisServer(ip, port, password, db), RedisServerConfig.default, name)
  })
  override val onNewSlave  =  (ip: String, port: Int) => {}
  override val onSlaveDown =  (ip: String, port: Int) => {}
}


case class SentinelMonitoredRedisBlockingClient( config: RedisConfiguration,
                                                 master: String,
                                                 password: Option[String] = None,
                                                 db: Option[Int] = None,
                                                 name: String = "SMRedisBlockingClient")
                                               (implicit system: ActorSystem,
                                                redisDispatcher: RedisDispatcher = Redis.dispatcher
                                                ) extends SentinelMonitoredRedisClientLike(config, system, redisDispatcher) with BLists {
  val redisClient: RedisBlockingClient = withMasterAddr((ip, port) => {
    RedisBlockingClient(RedisServer(ip, port, password, db), RedisServerConfig.default, name)
  })
  override val onNewSlave =  (ip: String, port: Int) => {}
  override val onSlaveDown =  (ip: String, port: Int) => {}
}

case class RedisDispatcher(name: String)

private[redis] object Redis {
  val dispatcher = RedisDispatcher("rediscala.rediscala-client-worker-dispatcher")

  val tempNumber = new AtomicLong

  def tempName() = Helpers.base64(tempNumber.getAndIncrement())

}
