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
import scala.collection.{Map => AnyMap}

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import org.apache.spark.SparkEnv

/**
 * Methods and implicits for convenience in working with Typesafe Config objects
 */
object ConfigUtils {
  def configFromMap(map: AnyMap[String, _]): Config = ConfigFactory.parseMap(map.asJava)

  val SparkDefaultConf = "org/apache/spark/spark-defaults.conf"
  val SparkConfigUrlProperty = "spark.config.url"
  // This may be used for global settings only as in the ones used in Singletons(object).
  // otherwise use the settings object propagated via sparkcontext.
  lazy val settings = new SparkEnv.Settings(ConfigUtils.loadConfig())
  // Change the default parse options so that missing files throw an exception, instead of returning empty
  val parseOptions = ConfigParseOptions.defaults.setAllowMissing(false)

  /**
   * Loads the Spark configuration according to the following priorities (what appears higher on the list
   * will override keys on the bottom)
   * 1. System properties
   * 2. config file (could be JSON) defined at URL in system property "spark.config.url", if defined
   *    Alternatively the environment variable SPARK_CONFIG_URL is also checked.
   * 3. spark-defaults.conf (in classpath / resources)
   */
  def loadConfig(): Config = {
    val properties = ConfigFactory.systemProperties()
    val defaults = ConfigFactory.parseResources(SparkDefaultConf, parseOptions)
    val configUrl = Option(System.getProperty(SparkConfigUrlProperty))
                      .orElse(Option(System.getenv("SPARK_CONFIG_URL")))
    configUrl match {
      case None =>
        defaults ++ properties
      case Some(configUrl) =>
        val javaUrl = new java.net.URL(configUrl)
        defaults ++ ConfigFactory.parseURL(javaUrl, parseOptions) ++ properties
    }
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
  def ++(other: Config): Config = other.withFallback(config)
  def ++(map: AnyMap[String, _]): Config = ConfigUtils.configFromMap(map).withFallback(config)

  /** config + ("some.key" -> value) */
  def +(keyValue: (String, Any)): Config = ++(Map(keyValue))

  /** Reads all the subkeys under a key, assumed to be a JSON object / map, and returns their values
   *  as a map of (subkey) -> value.toString
   */
  def getMap(key: String): Map[String, String] = {
    val entries = config.getObject(key).entrySet.asScala
    entries.map { entry => entry.getKey -> entry.getValue.unwrapped.toString }.toMap
  }
}
