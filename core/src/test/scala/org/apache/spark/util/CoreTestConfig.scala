package org.apache.spark.util

import com.typesafe.config.ConfigFactory

object CoreTestConfig {
   val config = ConfigFactory.parseString(
     """
       |spark.storage.memoryFraction = 0.66
       |spark.cleaner.ttl = 3600
     """.stripMargin)
}
