package org.apache.spark

import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}
import scala.reflect.ClassTag.{Boolean => CtagBool, Double => CtagDouble, Int => CtagInt,
  Long => CtagLong}

import com.typesafe.config.{ConfigFactory => CF, Config}

import org.apache.spark.SparkContext._

/*
 * INTERNAL API
 */
private[spark] class SparkConf {
  // internal mutable config.
  var config = CF.empty

  private[spark] def set[T: ClassTag](k: String, value: T) = {
    classTag[T] match {
      case CtagInt | CtagLong | CtagDouble | CtagBool =>
        config = config.withFallback(CF.parseString( s"""$k=$value """))
      case _ =>
        config = config.withFallback(CF.parseString( s"""$k="$value" """))
    }
    this
  }

  private[spark] def overrideWith(conf: Config) = {
    config = conf.withFallback(config)
    this
  }

  private[spark] def overrideWithMap(map: Map[String, _]) = {
    CF.parseMap(map.asInstanceOf[Map[String, Object]].asJava)
    this
  }

  private[spark] def internalConf = config
}


// An example usage
object ConfTest {
  val compiles = SparkConfBuilder().withMasterUrl("local").withAppName("name").build
  //  val doesNotCompile = SparkConfBuilder().withAppName("name").build
}