package org.apache.spark.streaming.util

import com.typesafe.config.ConfigFactory

/**
 * Default set of configuration for tests
 */
object StreamingTestConfig {
  final val config = ConfigFactory.parseString(
    """
      |spark.storage.memoryFraction = 0.66
      |spark.cleaner.ttl = 3600
      |spark.streaming.clock = "org.apache.spark.streaming.util.SystemClock"
      |spark.streaming.batchDuration = 10
    """.stripMargin)
}
