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

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}

/**
 * Methods and implicits for convenience in working with Typesafe Config objects
 */
object ConfigUtils {
  def configFromMap(map: collection.Map[String, _]): Config = ConfigFactory.parseMap(map.asJava)

  val SparkDefaultConf = "spark-defaults.conf"
  val SparkConfigUrlProperty = "spark.config.url"

  // Change the default parse options so that missing files throw an exception, instead of returning empty
  val parseOptions = ConfigParseOptions.defaults.setAllowMissing(false)

  /**
   * Loads the Spark configuration according to the following priorities (what appears higher on the list
   * will override keys on the bottom)
   * 1. System properties
   * 2. config file (could be JSON) defined at URL in system property "spark.config.url", if defined
   * 3. spark-defaults.conf (in classpath / resources)
   */
  def loadConfig(): Config = {
    val properties = ConfigFactory.systemProperties()
    val defaults = ConfigFactory.parseResources(SparkDefaultConf, parseOptions)
    System.getProperty(SparkConfigUrlProperty) match {
      case null =>
        properties.withFallback(defaults)
      case configUrl =>
        val javaUrl = new java.net.URL(configUrl)
        properties.withFallback(ConfigFactory.parseURL(javaUrl, parseOptions)).withFallback(defaults)
    }
  }

  implicit def config2RichConfig(config: Config): RichConfig = new RichConfig(config)
}

/**
 * Adds convenience methods for dealing with Typesafe Config objects
 */
class RichConfig(config: Config) {
  def ++(other: Config): Config = other.withFallback(config)
  def ++(map: collection.Map[String, _]): Config = ConfigUtils.configFromMap(map).withFallback(config)

  /** config + ("some.key" -> value) */
  def +(keyValue: (String, Any)): Config = ++(Map(keyValue))
}
