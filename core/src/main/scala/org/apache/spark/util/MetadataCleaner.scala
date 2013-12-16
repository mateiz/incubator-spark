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

import java.util.{Timer, TimerTask}

import org.apache.spark.Logging

/**
 * Runs a timer task to periodically clean up metadata (e.g. old files or hashtable entries)
 */
class MetadataCleaner(cleanerType: MetadataCleanerType.MetadataCleanerType, delaySeconds: Int,
                      cleanupFunc: (Long) => Unit) extends Logging {

  val name = cleanerType.toString
  private val periodSeconds = math.max(10, delaySeconds / 10)
  private val timer = new Timer(name + " cleanup timer", true)

  private val task = new TimerTask {
    override def run() {
      try {
        cleanupFunc(System.currentTimeMillis() - (delaySeconds * 1000))
        logInfo("Ran metadata cleaner for " + name)
      } catch {
        case e: Exception => logError("Error running cleanup task for " + name, e)
      }
    }
  }

  if (delaySeconds > 0) {
    logDebug(
      "Starting metadata cleaner for " + name + " with delay of " + delaySeconds + " seconds " +
      "and period of " + periodSeconds + " secs")
    timer.schedule(task, periodSeconds * 1000, periodSeconds * 1000)
  }

  def cancel() {
    timer.cancel()
  }
}

object MetadataCleanerType extends Enumeration {

  val MAP_OUTPUT_TRACKER, SPARK_CONTEXT, HTTP_BROADCAST, DAG_SCHEDULER, RESULT_TASK,
    SHUFFLE_MAP_TASK, BLOCK_MANAGER, SHUFFLE_BLOCK_MANAGER, BROADCAST_VARS = Value

  type MetadataCleanerType = Value
}
