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
import scala.collection.{Map => AnyMap}

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import org.apache.spark.SparkEnv

/**
 * Methods and implicits for convenience in working with Typesafe Config objects
 */
object ConfigUtils {
  def configFromMap(map: AnyMap[String, _]): Config = ConfigFactory.parseMap(map.asJava)

  val sparkDefaultConf = "spark.conf"
  // This may be used for global settings only as in the ones used in Singletons(object).
  // otherwise use the settings object propagated via SparkContext or SparkEnv.
  lazy val settings = new SparkEnv.Settings(ConfigUtils.loadConfig())
  val parseOptions = ConfigParseOptions.defaults.setAllowMissing(true)

  /**
   * Loads the Spark configuration in the order of their precedence.
   * 1. System properties (Highest precedence)
   * 2. spark.conf (in classpath / resources)
   */
  def loadConfig(): Config = {
    val properties = ConfigFactory.systemProperties()
    val defaults = ConfigFactory.parseResources(sparkDefaultConf, parseOptions)

    properties.withFallback(defaults)

  }

  implicit def config2RichConfig(config: Config): RichConfig = new RichConfig(config)

  /** Creates a Config from Spark master and appName strings */
  def configFromMasterAppName(master: String, appName: String): Config =
    configFromMap(Map("spark.master" -> master, "spark.appName" -> appName))

  /** Creates a Config from the spark home variable */
  def configFromSparkHome(sparkHome: String): Config = {
    if (sparkHome != null)
      configFromMap(Map("spark.home" -> sparkHome))
    else
      ConfigFactory.empty
  }

  /** Creates a Config from a list of jar URLs */
  def configFromJarList(jars: Seq[String]): Config = {
    if (jars != Nil)
      configFromMap(Map("spark.jars" -> jars.asJava))
    else
      ConfigFactory.empty
  }

  /** Creates a Config from a map of environment strings */
  def configFromEnvironmentMap(environment: AnyMap[String, String]): Config = {
    if (!environment.isEmpty)
      configFromMap(environment).atPath("spark.environment")
    else
      ConfigFactory.empty
  }
}

/**
 * Adds convenience methods for dealing with Typesafe Config objects
 */
class RichConfig(config: Config) {
  def overrideWithMap(map: AnyMap[String, _]): Config = ConfigUtils.configFromMap(map).withFallback(config)

  /** Reads all the subkeys under a key, assumed to be a JSON object / map, and returns their values
   *  as a map of (subkey) -> value.toString
   */
  def getMap(key: String): Map[String, String] = {
    val entries = config.getObject(key).entrySet.asScala
    entries.map { entry => entry.getKey -> entry.getValue.unwrapped.toString }.toMap
  }
}
