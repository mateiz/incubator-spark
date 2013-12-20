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

import com.typesafe.config.ConfigFactory

import org.scalatest.{FunSuite, BeforeAndAfter}

class ConfigUtilsSuite extends FunSuite with BeforeAndAfter {
  before {
    System.clearProperty("spark.driver.port")
    ConfigFactory.invalidateCaches()
  }

  test("loadConfig should let system properties override spark-defaults") {
    System.setProperty("spark.driver.port", "1313")
    ConfigFactory.invalidateCaches()
    val conf = ConfigUtils.loadConfig()
    assert(conf.getInt("spark.driver.port") === 1313)
  }

  test("configFromJarList") {
    val jars = Seq("http://abc.co/def.jar", "/path/to/bbb.jar")
    val config = ConfigUtils.configFromJarList(jars)
    assert(config.getStringList("spark.jars").asScala === jars)
  }

  test("configFromEnvironmentMap") {
    val env = Map("SPARK_CONF_DIR" -> "/etc/spark/conf",
                  "SPARK_HOME"     -> "/etc/spark")
    val config = ConfigUtils.configFromEnvironmentMap(env)

    import ConfigUtils._
    assert(config.getMap("spark.environment") === env)
  }

  test("can add configurations together") {
    import ConfigUtils._
    val jars = Seq("http://abc.co/def.jar", "/path/to/bbb.jar")
    val config = ConfigUtils.configFromJarList(jars)
    val config2 = config.withFallback(ConfigUtils.configFromSparkHome("/etc/spark"))
    assert(config2.getStringList("spark.jars").asScala === jars)
    assert(config2.getString("spark.home") === "/etc/spark")

    val config3 = config2.overrideWithMap(Map("spark.home" -> "/abc/def"))
    assert(config3.getString("spark.home") === "/abc/def")
  }
}