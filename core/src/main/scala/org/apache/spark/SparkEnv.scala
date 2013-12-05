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

import collection.mutable
import serializer.Serializer
import scala.util.Try

import akka.actor._
import akka.remote.RemoteActorRefProvider
import com.typesafe.config.{Config, ConfigFactory}

import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.storage.{BlockManagerMasterActor, BlockManager, BlockManagerMaster}
import org.apache.spark.network.ConnectionManager
import org.apache.spark.serializer.{Serializer, SerializerManager}
import org.apache.spark.util.{Utils, AkkaUtils}
import org.apache.spark.api.python.PythonWorkerFactory

import com.google.common.collect.MapMaker


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
  import collection.JavaConverters._

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
   * @param akkaHostPortFunction: returns the host and port for initializing the ActorSystem
   * @param isDriver true if this is the driver
   * @param isLocal  true if running in local mode (single process)
   */
  def createFromConfig(
      executorId: String,
      config: Config,
      akkaHostPortFunction: => (String, Int),
      isDriver: Boolean,
      isLocal: Boolean): SparkEnv = {
    import org.apache.spark.util.ConfigUtils._

    val settings: Settings = new Settings(config)
    val (akkaHost, akkaPort) = akkaHostPortFunction

    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", akkaHost, akkaPort, config)

    val driverConfig = if (isDriver) {
      Map("spark.driver.host" -> akkaHost, "spark.driver.port" -> boundPort)
    } else {
      Map.empty[String, Any]
    }

    val classLoader = Thread.currentThread.getContextClassLoader

    // Create an instance of the class named by the given Java system property, or by
    // defaultClassName if the property is not set, and return it as a T
    def instantiateClass[T](propertyName: String, defaultClassName: String): T = {
      val name = System.getProperty(propertyName, defaultClassName)
      Class.forName(name, true, classLoader).newInstance().asInstanceOf[T]
    }

    val serializerManager = new SerializerManager

    val serializer = serializerManager.setDefault(
      System.getProperty("spark.serializer", "org.apache.spark.serializer.JavaSerializer"))

    val closureSerializer = serializerManager.get(
      System.getProperty("spark.closure.serializer", "org.apache.spark.serializer.JavaSerializer"))

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
      "BlockManagerMaster",
      new BlockManagerMasterActor(isLocal)))

    val blockManager = new BlockManager(executorId, actorSystem, blockManagerMaster, serializer,
      settings)
    val connectionManager = blockManager.connectionManager

    val broadcastManager = new BroadcastManager(isDriver, settings)

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

    val shuffleFetcher = instantiateClass[ShuffleFetcher](
      "spark.shuffle.fetcher", "org.apache.spark.BlockStoreShuffleFetcher")

    val httpFileServer = new HttpFileServer()
    httpFileServer.initialize()
    System.setProperty("spark.fileserver.uri", httpFileServer.serverUri)

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

    val mergedConfig = config ++ driverConfig ++ broadcastManager.configUpdates

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
   * handled and the optional path be executed.
   */
  private[spark] class Settings(conf: Config) {

    import conf._

    final val memoryFraction = getDouble("spark.storage.memoryFraction")

    final def maxMemBlockManager = Try(getLong("spark.storage.blockmanager.maxmem"))
      .getOrElse((Runtime.getRuntime.maxMemory * memoryFraction).toLong)

    final val cleanerTtl = getInt("spark.cleaner.ttl")
    final val bufferSize = getInt("spark.buffer.size")
    final val compressBroadcast = getBoolean("spark.broadcast.compress")
    final val httpBroadcastURI = getString("spark.httpBroadcast.uri")
    final val broadCastFactory = getString("spark.broadcast.factory")
    final val blockSize = getInt("spark.broadcast.blockSize")
  }
}
