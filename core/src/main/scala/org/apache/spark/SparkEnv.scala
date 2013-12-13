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

import java.lang.System

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.ClassTag.{ Int => CtagInt, Long => CtagLong,
  Double => CtagDouble, Boolean => CtagBool}
import scala.util.Try

import akka.actor._
import com.google.common.collect.MapMaker
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.deploy.worker.ui.WorkerWebUI
import org.apache.spark.io.LZFCompressionCodec
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.ConnectionManager
import org.apache.spark.serializer.{Serializer, SerializerManager}
import org.apache.spark.storage.{BlockManager, BlockManagerMaster, BlockManagerMasterActor}
import org.apache.spark.ui.SparkUI
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
      hostName: String,
      isDriver: Boolean,
      isLocal: Boolean): SparkEnv = {
    import org.apache.spark.util.ConfigUtils._

    val httpFileServer = new HttpFileServer()
    httpFileServer.initialize()
    val conf = ConfigFactory.
      parseString( s"""spark.fileserver.uri = "${httpFileServer.serverUri}" """)
      .withFallback(config)

    var settings: Settings = new Settings(conf)
    val hostName2 = if (isDriver) settings.driverHost else hostName
    val port = if (isDriver) Try(settings.driverPort).getOrElse(0) else 0
    val (actorSystem: ActorSystem, boundPort: Int) =
      AkkaUtils.createActorSystem("spark", hostName2, port, settings)

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
      MetricsSystem.createMetricsSystem("driver", settings)
    } else {
      MetricsSystem.createMetricsSystem("executor", settings)
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
   * Note: Properties that do not have a default alternative can not be kept as val, use def
   * instead.
   */
  private[spark] class Settings(internalConfig: Config) {

    import scala.reflect.classTag

    private var mutableConf: Config = ConfigFactory.empty

    /**
     * This method will let one modify only the optional configuration even after spark context is created.
     * TODO: Decide whether this should be exposed to user or not.
     * @param propertyName
     * @param defaultValue
     * @tparam T
     * @return
     */
    private def configure[T: ClassTag](propertyName: String, defaultValue: T): T = {
      import internalConfig._

      val result = classTag[T] match {
        case CtagInt => Try(getInt(propertyName).asInstanceOf[T]).getOrElse(defaultValue)
        case CtagDouble => Try(getDouble(propertyName).asInstanceOf[T]).getOrElse(defaultValue)
        case CtagBool => Try(getBoolean(propertyName).asInstanceOf[T]).getOrElse(defaultValue)
        case CtagLong => Try(getLong(propertyName).asInstanceOf[T]).getOrElse(defaultValue)
        case _ => //Default is treated as String.
          Try(getString(propertyName).asInstanceOf[T]).getOrElse(defaultValue)
      }

      val con = classTag[T] match {
        case CtagInt | CtagLong | CtagDouble | CtagBool =>
          ConfigFactory.parseString(s"$propertyName=$result")
        case _ =>
          ConfigFactory.parseString( s"""$propertyName="$result" """)
      }

      mutableConf = con.withFallback(mutableConf)
      result
    }
    @throws[Exception]
    private def configure[T: ClassTag](propertyName: String): T = {
      import internalConfig._

      val result = classTag[T] match {
        case CtagInt => getInt(propertyName).asInstanceOf[T]
        case CtagDouble => getDouble(propertyName).asInstanceOf[T]
        case CtagBool => getBoolean(propertyName).asInstanceOf[T]
        case CtagLong => getLong(propertyName).asInstanceOf[T]
        case _ => //Default is treated as String.
          getString(propertyName).asInstanceOf[T]
      }
      val con = classTag[T] match {
        case CtagInt | CtagLong | CtagDouble | CtagBool =>
          ConfigFactory.parseString(s"$propertyName=$result")
        case _ =>
          ConfigFactory.parseString( s"""$propertyName="$result" """)
      }
      mutableConf = con.withFallback(mutableConf)
      result
    }
    /*All of the configurations are set as final so that they can not be overriden.*/
    final def sparkHome = Try(internalConfig.getString("spark.home")).toOption
      .orElse(Option(System.getenv("SPARK_HOME")))

    final val sparkLocalDir = configure("spark.local.dir", System.getProperty("java.io.tmpdir"))
    final val memoryFraction = configure("spark.storage.memoryFraction", 0.66)
    final val logConf = configure("spark.log.confAsInfo", false)
    final val sparkUser = Try(internalConfig.getString("user.name")).getOrElse(
      Option(System.getenv("SPARK_USER")).getOrElse(SparkContext.SPARK_UNKNOWN_USER))

    final val closureSerializer = configure("spark.closure.serializer",
      "org.apache.spark.serializer.JavaSerializer")

    final val serializer = configure("spark.serializer",
      "org.apache.spark.serializer.JavaSerializer")

    final val shuffleFetcher = configure("spark.shuffle.fetcher",
      "org.apache.spark.BlockStoreShuffleFetcher")

    final def useCompressedOops = configure[Boolean]("spark.test.useCompressedOops")

    final val retainedStages = configure("spark.ui.retained_stages", 1000)
    final val sparkUiPort = configure("spark.ui.port", SparkUI.DEFAULT_PORT)
    final val consolidateShuffleFiles = configure("spark.shuffle.consolidateFiles", true)
    final val shuffleBufferSize = configure("spark.shuffle.file.buffer.kb", 100)
    final val subDirsPerLocalDir = configure("spark.diskStore.subDirectories", 64)
    final val shuffleSyncWrites = configure("spark.shuffle.sync", false)
    final val shuffleCopierThreads = configure("spark.shuffle.copier.threads", 6)

    /** Only set at driver, the defaults are replaced once ActorSystem is initialized
      * and reflected in augmented settings created, which is used everywhere. */
    final val driverHost = configure("spark.driver.host", Utils.localHostName())

    final def driverPort = configure[Int]("spark.driver.port")

    final val cleanerTtl = configure("spark.cleaner.ttl", 3600)

    /** Maps to io.file.buffer.size */
    final val bufferSize = configure("spark.buffer.size", 65536)

    final def replClassUri = configure[String]("spark.repl.class.uri")

    final val executorMem = configure("spark.executor.memory", Option(System.getenv("SPARK_MEM"))
      .map(Utils.memoryStringToMb)
      .getOrElse(512))

    final def defaultParallelism = configure[Int]("spark.default.parallelism")

    final val zkWorkingDir = configure("spark.deploy.zookeeper.dir", "/spark")
    final val zkUrl = configure("spark.deploy.zookeeper.url", "")

    final def metricsConfFile = configure[String]("spark.metrics.conf")

    final val replDir = configure("spark.repl.classdir", System.getProperty("java.io.tmpdir"))

    final def executorUri = configure[String]("spark.executor.uri")

    final val resultGetterThreads = configure("spark.resultGetter.threads", 4)
    final val schedulerReviveInterval = configure("spark.scheduler.revive.interval", 1000l)

    // Kryo Related
    final val kryoReferenceTracking = configure("spark.kryo.referenceTracking", true)
    final val kryoSerializerBufferMb = configure("spark.kryoserializer.buffer.mb", 2)

    final def kryoRegistrator = Try(configure[String]("spark.kryo.registrator")).toOption

    // Broadcast Related
    final val compressBroadcast = configure("spark.broadcast.compress", true)
    final def httpBroadcastURI = configure[String]("spark.httpBroadcast.uri")
    final val broadCastFactory = configure("spark.broadcast.factory",
      "org.apache.spark.broadcast.HttpBroadcastFactory")

    final val blockSize = configure("spark.broadcast.blockSize", 4096)

    // Akka related
    final val akkaThreads = configure("spark.akka.threads", 4)
    final val akkaBatchSize = configure("spark.akka.batchSize", 15)
    final val akkaTimeout = configure("spark.akka.timeout", 100)
    final val akkaFrameSize = configure("spark.akka.frameSize", 10)
    final val lifecycleEvents = if (configure("spark.akka.logLifecycleEvents", false)) "on" else "off"
    final val akkaHeartBeatPauses = configure("spark.akka.heartbeat.pauses", 600)
    final val akkaFailureDetector = configure("spark.akka.failure-detector.threshold", 300.0)
    final val akkaHeartBeatInterval = configure("spark.akka.heartbeat.interval", 1000)
    final val askTimeout = configure("spark.akka.askTimeout", 10)

    //Scheduling related
    final val schedulingMode = configure("spark.scheduler.mode", "FIFO")
    final val mesosIsCoarse = configure("spark.mesos.coarse", false)
    final val simrMaxCores = configure("spark.simr.executor.cores", 1)
    final val maxCores = configure("spark.cores.max", Int.MaxValue)
    final val mesosExtraCoresPerSlave = configure("spark.mesos.extra.cores", 0)

    final def schedulerAllocationFile = configure[String]("spark.scheduler.allocation.file")

    //Blockmanager related
    final def maxMemBlockManager = configure("spark.storage.blockmanager.maxmem",
      (Runtime.getRuntime.maxMemory * memoryFraction).toLong)

    final val useNetty = configure("spark.shuffle.use.netty", false)
    final val nettyPortConfig = configure("spark.shuffle.sender.port", 0)
    final val maxMbInFlight = configure("spark.reducer.maxMbInFlight", 48l)
    final val compressShuffle = configure("spark.shuffle.compress", true)
    final val compressRdds = configure("spark.rdd.compress", false)
    final val bmTimeout = configure("spark.storage.blockManagerTimeoutIntervalMs", 60000l)
    final val slaveTimeout = configure("spark.storage.blockManagerSlaveTimeoutMs", bmTimeout * 3l)
    final val disableTestHeartBeat = configure("spark.test.disableBlockManagerHeartBeat", false)
    final val akkaRetries = configure("spark.akka.num.retries", 3)
    final val akkaRetryInterval = configure("spark.akka.retry.wait", 3000)

    //Task related
    final val localityWait = configure("spark.locality.wait", 3000l)
    final val cpuPerTask = configure("spark.task.cpus", 1)
    final val taskMaxFailures = configure("spark.task.maxFailures", 4)
    final val speculationMultiplier = configure("spark.speculation.multiplier", 1.5)

    final val speculationQuantile = configure("spark.speculation.quantile", 0.75)
    final val speculationInterval = configure("spark.speculation.interval", 100l)
    final val speculation = configure("spark.speculation", false)

    final val starvationTimeout = configure("spark.starvation.timeout", 15000l)

    final val localityWaitProcess = configure("spark.locality.wait.process", localityWait)
    final val localityWaitNode = configure("spark.locality.wait.node", localityWait)
    final val localityWaitRack = configure("spark.locality.wait.rack", localityWait)
    final val exceptionPrintInterval = configure("spark.logging.exceptionPrintInterval", 10000l)
    final val shuffleNettyConnectTimeout = configure("spark.shuffle.netty.connect.timeout", 60000)

    //Compression related
    final val compressionCodec = configure("spark.io.compression.codec",
      classOf[LZFCompressionCodec].getName)
    final val snappyCompressionBlockSize = configure("spark.io.compression.snappy.block.size", 32768)
    //Connection Manager related
    final val handlerMinThreads = configure("spark.core.connection.handler.threads.min", 20)
    final val handlerMaxThreads = configure("spark.core.connection.handler.threads.max", 60)
    final val handlerKeepalive = configure("spark.core.connection.handler.threads.keepalive", 60)
    final val ioMinThreads = configure("spark.core.connection.handler.io.min", 4)
    final val ioMaxThreads = configure("spark.core.connection.handler.io.max", 32)
    final val ioKeepalive = configure("spark.core.connection.handler.io.keepalive", 60)
    final val connectMinThreads = configure("spark.core.connection.connect.threads.min", 1)
    final val connectMaxThreads = configure("spark.core.connection.connect.threads.max", 8)
    final val connectKeepalive = configure("spark.core.connection.connect.threads.keepalive", 60)

    //deploy related
    final val workerTimeout = configure("spark.worker.timeout", 60l)
    final val retainedApplication = configure("spark.deploy.retainedApplications", 200)
    final val workerPersistence = configure("spark.dead.worker.persistence", 15)
    final val recoveryDir = configure("spark.deploy.recoveryDirectory", "")
    final val recoveryMode = configure("spark.deploy.recoveryMode", "NONE")
    final val spreadOutApps = configure("spark.deploy.spreadOut", true)

    final def masterUiPort = configure[Int]("master.ui.port")

    final val workerUiPort = configure("worker.ui.port", WorkerWebUI.DEFAULT_PORT)

    //Streaming
    final val blockInterval = configure("spark.streaming.blockInterval", 200l)
    final val concurrentJobs = configure("spark.streaming.concurrentJobs", 1)
    final val clockClass = configure("spark.streaming.clock",
      "org.apache.spark.streaming.util.SystemClock")
    final val jumpTime = configure("spark.streaming.manualClock.jump", 0l)

    //yarn
    final val yarnReplication = configure("spark.yarn.submit.file.replication", 3).toShort
    final val yarnUserClasspathFirst = configure("spark.yarn.user.classpath.first", false)

    final def yarnMaxNumWorkerFailures = configure[Int]("spark.yarn.max.worker.failures")

    final val yarnWaitTries = configure("spark.yarn.applicationMaster.waitTries", 10)
    final val yarnSchedulerInterval = configure("spark.yarn.scheduler.heartbeat.interval-ms",
      5000l)
    final val yarnPreserveFiles = configure("spark.yarn.preserve.staging.files", false)

    override def toString = mutableConf.root().render()

    def config = mutableConf.withFallback(internalConfig)
  }
}
