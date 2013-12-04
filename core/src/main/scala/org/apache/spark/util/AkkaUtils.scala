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

package org.apache.spark.util

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.remote.RemoteActorRefProvider

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Various utility classes for working with Akka.
 */
private[spark] object AkkaUtils {

  /**
   * Creates an ActorSystem ready for remoting, with various Spark features. Returns both the
   * ActorSystem itself and its port (which is hard to get from Akka).
   *
   * Note: the `name` parameter is important, as even if a client sends a message to right
   * host + port, if the system name is incorrect, Akka will drop the message.
   */
  def createActorSystem(name: String, host: String, port: Int, 
    config: Config = ConfigUtils.loadConfig()): (ActorSystem, Int) = {
    val akkaThreads = config.getInt("spark.akka.threads")
    val akkaBatchSize = config.getInt("spark.akka.batchSize")
    val akkaTimeout = config.getInt("spark.akka.timeout")
    val akkaFrameSize = config.getInt("spark.akka.frameSize")
    val lifecycleEvents = if (config.getBoolean("spark.akka.logLifecycleEvents")) "on" else "off"
    val akkaHeartBeatPauses = config.getInt("spark.akka.heartbeat.pauses")
    val akkaFailureDetector = config.getDouble("spark.akka.failure-detector.threshold")
    val akkaHeartBeatInterval = config.getInt("spark.akka.heartbeat.interval")

    val akkaConf = ConfigFactory.parseString(
      s"""
      |akka.daemonic = on
      |akka.loggers = [""akka.event.slf4j.Slf4jLogger""]
      |akka.stdout-loglevel = "ERROR"
      |akka.jvm-exit-on-fatal-error = off
      |akka.remote.transport-failure-detector.heartbeat-interval = $akkaHeartBeatInterval s
      |akka.remote.transport-failure-detector.acceptable-heartbeat-pause = $akkaHeartBeatPauses s
      |akka.remote.transport-failure-detector.threshold = $akkaFailureDetector
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
      |akka.remote.netty.tcp.hostname = "$host"
      |akka.remote.netty.tcp.port = $port
      |akka.remote.netty.tcp.tcp-nodelay = on
      |akka.remote.netty.tcp.connection-timeout = $akkaTimeout s
      |akka.remote.netty.tcp.maximum-frame-size = ${akkaFrameSize}MiB
      |akka.remote.netty.tcp.execution-pool-size = $akkaThreads
      |akka.actor.default-dispatcher.throughput = $akkaBatchSize
      |akka.remote.log-remote-lifecycle-events = $lifecycleEvents
      """.stripMargin)
    val actorSystem = ActorSystem(name, akkaConf.withFallback(config))

    val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider
    val boundPort = provider.getDefaultAddress.port.get
    (actorSystem, boundPort)
  }

}
