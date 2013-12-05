package org.apache.spark.util

import com.typesafe.config.ConfigFactory

object CoreTestConfig {
   val config = ConfigFactory.parseString(
     """
       |spark.storage.memoryFraction = 0.66
       |spark.cleaner.ttl = 3600
       |spark.buffer.size = 65536
       |spark.broadcast.blockSize = 4096
       |spark.broadcast.factory = "org.apache.spark.broadcast.HttpBroadcastFactory"
       |spark.httpBroadcast.uri = ""
       |spark.broadcast.compress = true
     """.stripMargin)
}
