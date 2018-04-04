package redis

import java.net.InetSocketAddress

import akka.actor.{ActorRef, ActorSystem, Props}

import scala.concurrent.stm._
import redis.actors.RedisClientActor

import scala.concurrent.{ExecutionContext, Future}
import redis.protocol.RedisReply
import redis.commands.Transactions

import scala.concurrent.duration._

case class RedisServer(host: String = "localhost",
                       port: Int = 6379,
                       password: Option[String] = None,
                       db: Option[Int] = None)


case class RedisConnection(actor: ActorRef, active: Ref[Boolean] = Ref(false))

abstract class RedisClientPoolLike(system: ActorSystem, redisDispatcher: RedisDispatcher)  {

  def redisServerConnections: scala.collection.Map[RedisServer, RedisConnection]

  val name: String
  implicit val executionContext = system.dispatchers.lookup(redisDispatcher.name)
  val connectTimeout: FiniteDuration = 1 seconds

  private val redisConnectionRef: Ref[Seq[ActorRef]] = Ref(Seq.empty)
  /**
    *
    * @param redisCommand
    * @tparam T
    * @return behave nicely with Future helpers like firstCompletedOf or sequence
    */
  def broadcast[T](redisCommand: RedisCommand[_ <: RedisReply, T]): Seq[Future[T]] = {
    redisConnectionPool.map(redisConnection => {
      send(redisConnection, redisCommand)
    })
  }

  protected def send[T](redisConnection: ActorRef, redisCommand: RedisCommand[_ <: RedisReply, T]): Future[T]

  def getConnectionsActive: Seq[ActorRef] = {
    redisServerConnections.collect {
      case (redisServer, redisConnection) if redisConnection.active.single.get => redisConnection.actor
    }.toVector
  }

  def redisConnectionPool: Seq[ActorRef] = {
    redisConnectionRef.single.get
  }

  def onConnect(redis: RedisCommands, server: RedisServer): Unit = {
    server.password.foreach(redis.auth(_)) // TODO log on auth failure
    server.db.foreach(redis.select)
  }

  def onConnectStatus(server: RedisServer, active: Ref[Boolean]): (Boolean) => Unit = {
    (status: Boolean) => {
        if (active.single.compareAndSet(!status, status)) {
          refreshConnections()
        }
      }
  }

  def refreshConnections() = {
    val actives = getConnectionsActive
    redisConnectionRef.single.set(actives)
  }

  def getConnectOperations(server: RedisServer): () => Seq[Operation[_, _]] = () => {
    val self = this
    val redis = new BufferedRequest with RedisCommands {
      implicit val executionContext: ExecutionContext = self.executionContext
    }
    onConnect(redis, server)
    redis.operations.result()
  }

  /**
   * Disconnect from the server (stop the actor)
   */
  def stop() {
    redisConnectionPool.foreach { redisConnection =>
      system stop redisConnection
    }
  }

  def makeRedisConnection(server: RedisServer, config: RedisServerConfig, defaultActive: Boolean = false) = {
    val active = Ref(defaultActive)
    (server, RedisConnection(makeRedisClientActor(server, config, active), active))
  }

  def makeRedisClientActor(server: RedisServer, config: RedisServerConfig, active: Ref[Boolean]): ActorRef = {
    system.actorOf(RedisClientActor.props(server, config,
      getConnectOperations(server), onConnectStatus(server, active), redisDispatcher.name)
      .withDispatcher(redisDispatcher.name),
      name + '-' + Redis.tempName()
    )
  }

}

case class RedisClientMutablePool(config: RedisConfiguration,
                                  name: String = "RedisClientPool")
                                 (implicit system: ActorSystem,
                                  redisDispatcher: RedisDispatcher = Redis.dispatcher
                                  ) extends RedisClientPoolLike (system, redisDispatcher) with RoundRobinPoolRequest with RedisCommands {

  override val redisServerConnections = {
    val m = config.topology.nodes map { server => makeRedisConnection(server, config.config) }
    collection.mutable.Map(m: _*)
  }

  def addServer(server: RedisServer, config: RedisServerConfig) {
    if (!redisServerConnections.contains(server)) {
      redisServerConnections.synchronized {
        if (!redisServerConnections.contains(server)) {
          redisServerConnections += makeRedisConnection(server, config)
        }
      }
    }
  }

  def removeServer(askServer: RedisServer) {
    if (redisServerConnections.contains(askServer)) {
      redisServerConnections.synchronized {
        redisServerConnections.get(askServer).foreach { redisServerConnection =>
          system stop redisServerConnection.actor
        }
        redisServerConnections.remove(askServer)
        refreshConnections()
      }
    }
  }


}

case class RedisClientPool(redisServers: Seq[RedisServer], config: RedisServerConfig,
                           name: String = "RedisClientPool")
                          (implicit _system: ActorSystem,
                           redisDispatcher: RedisDispatcher = Redis.dispatcher
                          ) extends RedisClientPoolLike(_system, redisDispatcher) with RoundRobinPoolRequest with RedisCommands {

  override val redisServerConnections = {
    redisServers.map { server =>
      makeRedisConnection(server, config, defaultActive = true)
    } toMap
  }

  refreshConnections()

}

case class RedisClientMasterSlaves(master: RedisServer,
                                   slaves: Seq[RedisServer], config: RedisServerConfig)
                                  (implicit _system: ActorSystem,
                                  redisDispatcher: RedisDispatcher = Redis.dispatcher)
                                  extends RedisCommands with Transactions {
  implicit val executionContext = _system.dispatchers.lookup(redisDispatcher.name)

  val masterClient = RedisClient(master)

  val slavesClients = RedisClientPool(slaves, config)

  override def send[T](redisCommand: RedisCommand[_ <: RedisReply, T]): Future[T] = {
    if (redisCommand.isMasterOnly || slaves.isEmpty) {
      masterClient.send(redisCommand)
    } else {
      slavesClients.send(redisCommand)
    }
  }

  def redisConnection: ActorRef = masterClient.redisConnection
}


case class SentinelMonitoredRedisClientMasterSlaves(config: RedisConfiguration, master: String)
    (implicit _system: ActorSystem, redisDispatcher: RedisDispatcher)
  extends SentinelMonitored(config, _system, redisDispatcher) with ActorRequest with RedisCommands with Transactions {

  val masterClient: RedisClient = withMasterAddr(
    (ip, port) => {
      new RedisClient(RedisServer(ip, port), RedisServerConfig.default, name = "SMRedisClient")
    })

  val slavesClients: RedisClientMutablePool = withSlavesAddr(
    slavesHostPort => {
      val slaves = slavesHostPort.map {
        case (ip, port) =>
          RedisServer(ip, port)
      }
      RedisClientMutablePool(config, name = "SMRedisClient")
    })


  val onNewSlave = (ip: String, port: Int) => {
    log.info(s"onNewSlave $ip:$port")
    slavesClients.addServer(RedisServer(ip, port), RedisServerConfig.default)
  }

  val onSlaveDown = (ip: String, port: Int) => {
    log.info(s"onSlaveDown $ip:$port")
    slavesClients.removeServer(RedisServer(ip, port))
  }

  val onMasterChange = (ip: String, port: Int) => {
    log.info(s"onMasterChange $ip:$port")
    masterClient.reconnect(ip, port)
  }

  /**
   * Disconnect from the server (stop the actors)
   */
  def stop() = {
    masterClient.stop()
    slavesClients.stop()
    sentinelClients.values.foreach(_.stop())
  }

  def redisConnection: ActorRef = masterClient.redisConnection

  override def send[T](redisCommand: RedisCommand[_ <: RedisReply, T]): Future[T] = {
    if (redisCommand.isMasterOnly || slavesClients.redisConnectionPool.isEmpty) {
      masterClient.send(redisCommand)
    } else {
      slavesClients.send(redisCommand)
    }
  }
}

object SentinelMonitoredRedisClientMasterSlaves {
  def apply(master: String)(implicit _system: ActorSystem, redisDispatcher: RedisDispatcher = Redis.dispatcher): SentinelMonitoredRedisClientMasterSlaves =
    SentinelMonitoredRedisClientMasterSlaves(RedisConfiguration(Seq[RedisServer](RedisServer("localhost", 26379))), master)
}
