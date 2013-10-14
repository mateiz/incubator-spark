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

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

import com.typesafe.config.{Config, ConfigFactory}

/**
 * A bunch of implicit conversions to add methods for convenience in working with Typesafe Config objects
 */
object ConfigUtils {
  def configFromMap(map: collection.Map[String, _]): Config = ConfigFactory.parseMap(map.asJava)

  val SparkDefaultConf = "spark-defaults.conf"

  /**
   * Loads the Spark configuration according to the following priorities (what appears higher on the list
   * will override keys on the bottom)
   * 1. System properties
   * 2. spark-defaults.conf (in classpath / resources)
   */
  def loadConfig(): Config = {
    val properties = ConfigFactory.systemProperties()
    properties.withFallback(ConfigFactory.parseResources(SparkDefaultConf))
  }
}

/**
 * A wrapper for config object which stores updates.  Since the config object is immutable,
 * the updates are stored in a separate hashmap, then the merge method can be called to produce
 * a newer, merged, immutable config.
 */
class ConfigUpdater(val config: Config) {
  private val updates = new HashMap[String, Any]

  def addUpdate(key: String, value: Any) {
    updates(key) = value
  }

  def merge(): Config = {
    ConfigUtils.configFromMap(updates).withFallback(config)
  }
}

