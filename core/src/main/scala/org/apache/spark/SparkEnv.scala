/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import scala.collection.mutable
import scala.util.Try

import akka.actor._
import com.google.common.collect.MapMaker
import com.typesafe.config.{Config, ConfigFactory}

import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.ConnectionManager
import org.apache.spark.serializer.{Serializer, SerializerManager}
import org.apache.spark.storage.{BlockManager, BlockManagerMaster, BlockManagerMasterActor}
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, Akka actor system, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a thread-local variable, so each thread that accesses these
 * objects needs to have the right SparkEnv set. You can get the current environment with
 * SparkEnv.get (e.g. after creating a SparkContext) and set it with SparkEnv.set.
 */
class SparkEnv (
    val conf: Config,
    val executorId: String,
    val actorSystem: ActorSystem,
    val serializerManager: SerializerManager,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val cacheManager: CacheManager,
    val mapOutputTracker: MapOutputTracker,
    val shuffleFetcher: ShuffleFetcher,
    val broadcastManager: BroadcastManager,
    val blockManager: BlockManager,
    val connectionManager: ConnectionManager,
    val httpFileServer: HttpFileServer,
    val sparkFilesDir: String,
    val metricsSystem: MetricsSystem,
    val settings: SparkEnv.Settings) {

  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
  private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()

  def stop() {
    pythonWorkers.foreach { case(key, worker) => worker.stop() }
    httpFileServer.stop()
    mapOutputTracker.stop()
    shuffleFetcher.stop()
    broadcastManager.stop()
    blockManager.stop()
    blockManager.master.stop()
    metricsSystem.stop()
    actorSystem.shutdown()
    // Unfortunately Akka's awaitTermination doesn't actually wait for the Netty server to shut
    // down, but let's call it anyway in case it gets fixed in a later release
    // UPDATE: In Akka 2.1.x, this hangs if there are remote actors, so we can't call it.
    //actorSystem.awaitTermination()
  }

  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }
}

object SparkEnv extends Logging {

  private val env = new ThreadLocal[SparkEnv]
  @volatile private var lastSetSparkEnv : SparkEnv = _

  def set(e: SparkEnv) {
	  lastSetSparkEnv = e
    env.set(e)
  }

  /**
   * Returns the ThreadLocal SparkEnv, if non-null. Else returns the SparkEnv
   * previously set in any thread.
   */
  def get: SparkEnv = {
    Option(env.get()).getOrElse(lastSetSparkEnv)
  }

  /**
   * Returns the ThreadLocal SparkEnv.
   */
  def getThreadLocal : SparkEnv = {
	  env.get()
  }

  /**
   * Creates a SparkEnv from configuration.
   * The config object passed in is not modified (all Typesafe Config objects are immutable).  However,
   * updates are merged into a new config object and passed into SparkEnv.
   * @param executorId 0 for driver, or the executer ID
   * @param config the Typesafe Config object to be used for configuring Spark
   * @param isDriver true if this is the driver
   * @param isLocal  true if running in local mode (single process)
   */
  private[spark] def createFromConfig(
      executorId: String,
      config: Config,
      isDriver: Boolean,
      isLocal: Boolean): SparkEnv = {
    import org.apache.spark.util.ConfigUtils._

    val httpFileServer = new HttpFileServer()
    httpFileServer.initialize()
    val conf = ConfigFactory.
      parseString(s"""spark.fileserver.uri = "${httpFileServer.serverUri}" """).withFallback(config)

    var settings: Settings = new Settings(conf)
    val hostName = if(isDriver) settings.driverHost else Utils.localHostName()
    val port = if(isDriver) Try(settings.driverPort).getOrElse(0) else 0
    val (actorSystem :ActorSystem, boundPort: Int) = AkkaUtils.createActorSystem("spark",hostName,port,settings)

    val driverConfig = if (isDriver) {
      Map("spark.driver.host" -> settings.driverHost, "spark.driver.port" -> boundPort)
    } else {
      Map.empty[String, Any]
    }

    val broadcastManager = new BroadcastManager(isDriver, settings)
    val mergedConfig = conf ++ driverConfig ++ broadcastManager.configUpdates

    settings = new Settings(mergedConfig) //Augmenting config with new settings.

    val classLoader = Thread.currentThread.getContextClassLoader

    // Create an instance of the class named by the given Java system property, or by
    // defaultClassName if the property is not set, and return it as a T
    def instantiateClass[T](className: String): T = {
      Class.forName(className, true, classLoader).newInstance().asInstanceOf[T]
    }

    val serializerManager = new SerializerManager

    val serializer = serializerManager.setDefault(settings.serializer)

    val closureSerializer = serializerManager.get(settings.closureSerializer)

    def registerOrLookup(name: String, newActor: => Actor): Either[ActorRef, ActorSelection] = {
      if (isDriver) {
        logInfo("Registering " + name)
        Left(actorSystem.actorOf(Props(newActor), name = name))
      } else {
        // Not a driver, get the driver host/port from passed in config
        val driverHost = Try(config.getString("spark.driver.host")).getOrElse("localhost")
        val driverPort = Try(config.getInt("spark.driver.port")).getOrElse("7077")
        Utils.checkHost(driverHost, "Expected hostname")
        val url = "akka.tcp://spark@%s:%s/user/%s".format(driverHost, driverPort, name)
        logInfo("Connecting to " + name + ": " + url)
        Right(actorSystem.actorSelection(url))
      }
    }
    val blockManagerMaster = new BlockManagerMaster(registerOrLookup(
      "BlockManagerMaster", new BlockManagerMasterActor(isLocal, settings)), settings)

    val blockManager = new BlockManager(executorId, actorSystem, blockManagerMaster, serializer,
      settings)
    val connectionManager = blockManager.connectionManager

    val cacheManager = new CacheManager(blockManager)

    // Have to assign trackerActor after initialization as MapOutputTrackerActor
    // requires the MapOutputTracker itself
    val mapOutputTracker =  if (isDriver) {
      new MapOutputTrackerMaster(settings)
    } else {
      new MapOutputTracker(settings)
    }
    mapOutputTracker.trackerActor = registerOrLookup(
      "MapOutputTracker",
      new MapOutputTrackerMasterActor(mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]))

    val shuffleFetcher = instantiateClass[ShuffleFetcher](settings.shuffleFetcher)

    val metricsSystem = if (isDriver) {
      MetricsSystem.createMetricsSystem("driver")
    } else {
      MetricsSystem.createMetricsSystem("executor")
    }
    metricsSystem.start()

    // Set the sparkFiles directory, used when downloading dependencies.  In local mode,
    // this is a temporary directory; in distributed mode, this is the executor's current working
    // directory.
    val sparkFilesDir: String = if (isDriver) {
      Utils.createTempDir().getAbsolutePath
    } else {
      "."
    }
    // Warn about deprecated spark.cache.class property
    if (System.getProperty("spark.cache.class") != null) {
      logWarning("The spark.cache.class property is no longer being used! Specify storage " +
      "levels using the RDD.persist() method instead.")
    }


    new SparkEnv(
      mergedConfig,
      executorId,
      actorSystem,
      serializerManager,
      serializer,
      closureSerializer,
      cacheManager,
      mapOutputTracker,
      shuffleFetcher,
      broadcastManager,
      blockManager,
      connectionManager,
      httpFileServer,
      sparkFilesDir,
      metricsSystem,
      settings)
  }

  /**
   * Optional configurations are defined as def for they will raise exception which can be
   * handled and the optional path be taken.
   */
  private[spark] class Settings(val conf: Config) {

    import conf._

    final def sparkHome = Try(getString("spark.home")).toOption
      .orElse(Option(System.getenv("SPARK_HOME")))
    final val sparkLocalDir = Try(getString("spark.local.dir")).
      getOrElse(System.getProperty("java.io.tmpdir"))
    final val memoryFraction = Try(getDouble("spark.storage.memoryFraction")).getOrElse(0.66)
    final val logConf = Try(getBoolean("spark.log.confAsInfo")).getOrElse(false)
    final val sparkUser = Try(getString("user.name")).
      getOrElse(Option(System.getenv("SPARK_USER")).getOrElse(SparkContext.SPARK_UNKNOWN_USER))

    final val closureSerializer = Try(getString("spark.closure.serializer")).
      getOrElse("org.apache.spark.serializer.JavaSerializer")

    final val serializer = Try(getString("spark.serializer")).
      getOrElse("org.apache.spark.serializer.JavaSerializer")

    final val shuffleFetcher = Try(getString("spark.shuffle.fetcher"))
      .getOrElse("org.apache.spark.BlockStoreShuffleFetcher")

    /** Only set at driver, the defaults are replaced once ActorSystem is initialized
      * and reflected in augmented settings created, which is used everywhere.*/
    final val driverHost = Try(getString("spark.driver.host")).getOrElse(Utils.localHostName())
    final def driverPort = getInt("spark.driver.port")

    final val cleanerTtl = Try(getInt("spark.cleaner.ttl")).getOrElse(3600)

    /** Maps to io.file.buffer.size */
    final val bufferSize = Try(getInt("spark.buffer.size")).getOrElse(65536)

    final def replClassUri = getString("spark.repl.class.uri")

    final val executorMem = Try(getString("spark.executor.memory")).toOption
      .orElse(Option(System.getenv("SPARK_MEM")))
      .map(Utils.memoryStringToMb)
      .getOrElse(512)

    // Broadcast Related
    final val compressBroadcast = Try(getBoolean("spark.broadcast.compress")).getOrElse(true)
    final val httpBroadcastURI = Try(getString("spark.httpBroadcast.uri")).getOrElse("")
    final val broadCastFactory = Try(getString("spark.broadcast.factory")).
      getOrElse("org.apache.spark.broadcast.HttpBroadcastFactory")

    final val blockSize = Try(getInt("spark.broadcast.blockSize")).getOrElse(4096)

    // Akka related
    final val akkaThreads = Try(getInt("spark.akka.threads")).getOrElse(4)
    final val akkaBatchSize = Try(getInt("spark.akka.batchSize")).getOrElse(15)
    final val akkaTimeout = Try(getInt("spark.akka.timeout")).getOrElse(60)
    final val akkaFrameSize = Try(getInt("spark.akka.frameSize")).getOrElse(10)
    final val lifecycleEvents = if (Try(getBoolean("spark.akka.logLifecycleEvents"))
      .getOrElse(false)) "on" else "off"
    final val akkaHeartBeatPauses = Try(getInt("spark.akka.heartbeat.pauses")).getOrElse(600)
    final val akkaFailureDetector = Try(getDouble("spark.akka.failure-detector.threshold")).
      getOrElse(300.0)
    final val akkaHeartBeatInterval = Try(getInt("spark.akka.heartbeat.interval")).getOrElse(1000)
    final val askTimeout = Try(getInt("spark.akka.askTimeout")).getOrElse(10)
    
    //Scheduling related
    final val schedulingMode = Try(getString("spark.scheduler.mode")).getOrElse("FIFO")
    final val mesosIsCoarse = Try(getBoolean("spark.mesos.coarse")).getOrElse(false)
    final val simrMaxCores = Try(getInt("spark.simr.executor.cores")).getOrElse(1)
    final val maxCores = Try(getInt("spark.cores.max")).getOrElse(Int.MaxValue)
    final val mesosExtraCoresPerSlave = Try(getInt("spark.mesos.extra.cores")).getOrElse(0)

    //Blockmanager related
    final def maxMemBlockManager = Try(getLong("spark.storage.blockmanager.maxmem"))
      .getOrElse((Runtime.getRuntime.maxMemory * memoryFraction).toLong)
    final val useNetty = Try(getBoolean("spark.shuffle.use.netty")).getOrElse(false)
    final val nettyPortConfig = Try(getInt("spark.shuffle.sender.port")).getOrElse(0)
    final val maxMbInFlight = Try(getLong("spark.reducer.maxMbInFlight")). //TODO: should be maxBytes
      getOrElse(48l)
    final val compressShuffle = Try(getBoolean("spark.shuffle.compress")).getOrElse(true)
    final val compressRdds = Try(getBoolean("spark.rdd.compress")).getOrElse(false)
    final val bmTimeout = Try(getLong("spark.storage.blockManagerTimeoutIntervalMs")).
      getOrElse(60000l)
    final val slaveTimeout = Try(getLong("spark.storage.blockManagerSlaveTimeoutMs")).
      getOrElse(bmTimeout * 3l)
    final val disableTestHeartBeat = Try(getBoolean("spark.test.disableBlockManagerHeartBeat")).
      getOrElse(false)
    final val akkaRetries: Int = Try(getInt("spark.akka.num.retries")).getOrElse(3)
    final val akkaRetryInterval: Int = Try(getInt("spark.akka.retry.wait")).getOrElse(3000)

    //Task related
    final val localityWait = Try(getLong("spark.locality.wait")).getOrElse(3000l)
    final val cpuPerTask = Try(getInt("spark.task.cpus")).getOrElse(1)
    final val taskMaxFailures = Try(getInt("spark.task.maxFailures")).getOrElse(4)
    final val speculationMultiplier = Try(getDouble("spark.speculation.multiplier")).getOrElse(1.5)

    final val speculationQuantile = Try(getDouble("spark.speculation.quantile")).getOrElse(0.75)
    final val speculationInterval = Try(getLong("spark.speculation.interval")).getOrElse(100l)
    final val speculation = Try(getBoolean("spark.speculation")).getOrElse(false)

    final val starvationTimeout = Try(getLong("spark.starvation.timeout")).getOrElse(15000l)

    final val localityWaitProcess = Try(getLong("spark.locality.wait.process")).
      getOrElse(localityWait)
    final val localityWaitNode = Try(getLong("spark.locality.wait.node")).getOrElse(localityWait)
    final val localityWaitRack = Try(getLong("spark.locality.wait.rack")).getOrElse(localityWait)
    final val exceptionPrintInterval = Try(getLong("spark.logging.exceptionPrintInterval")).
      getOrElse(10000l)

    //Connection Manager related
    final val handlerMinThreads = Try(getInt("spark.core.connection.handler.threads.min")).
      getOrElse(20)
    final val handlerMaxThreads = Try(getInt("spark.core.connection.handler.threads.max")).
      getOrElse(60)
    final val handlerKeepalive = Try(getInt("spark.core.connection.handler.threads.keepalive")).
      getOrElse(60)
    final val ioMinThreads = Try(getInt("spark.core.connection.handler.io.min")).getOrElse(4)
    final val ioMaxThreads = Try(getInt("spark.core.connection.handler.io.max")).getOrElse(32)
    final val ioKeepalive = Try(getInt("spark.core.connection.handler.io.keepalive")).getOrElse(60)
    final val connectMinThreads = Try(getInt("spark.core.connection.connect.threads.min")).
      getOrElse(1)
    final val connectMaxThreads = Try(getInt("spark.core.connection.connect.threads.max")).
      getOrElse(8)
    final val connectKeepalive = Try(getInt("spark.core.connection.connect.threads.keepalive")).
      getOrElse(60)

    //deploy related
    final val workerTimeout = Try(getLong("spark.worker.timeout")).getOrElse(60l)
    final val retainedApplication = Try(getInt("spark.deploy.retainedApplications")).getOrElse(200)
    final val workerPersistence = Try(getInt("spark.dead.worker.persistence")).getOrElse(15)
    final val recoveryDir = Try(getString("spark.deploy.recoveryDirectory")).getOrElse("")
    final val recoveryMode = Try(getString("spark.deploy.recoveryMode")).getOrElse("NONE")
    final val spreadOutApps = Try(getBoolean("spark.deploy.spreadOut")).getOrElse(true)

    //Streaming
    final val blockInterval = Try(getLong("spark.streaming.blockInterval")).getOrElse(200l)
    final val concurrentJobs = Try(getInt("spark.streaming.concurrentJobs")).getOrElse(1)
    final val clockClass = Try(getString("spark.streaming.clock")).
      getOrElse("org.apache.spark.streaming.util.SystemClock")
    final val jumpTime = Try(getLong("spark.streaming.manualClock.jump")).getOrElse(0l)

    
  }
}
