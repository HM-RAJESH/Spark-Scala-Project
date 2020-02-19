package com.rajesh.task

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkWordCountTest extends FunSuite with BeforeAndAfter {
  var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setAppName("Test SparkWordCount")
      .setMaster("local")

    sc = new SparkContext(conf)
  }

  test("SparkWordCount Test") 
  {
    val in: RDD[String] = sc.parallelize(Seq("The quick brown fox jumps over the lazy brown dog."))
    val out: Array[(String, Int)] = Array(("brown", 2), ("dog", 1), ("fox", 1), ("jumps", 1), ("lazy", 1), ("over", 1), ("quick", 1), ("the", 2))
    SparkWordCount.setSparkContext(sc)
    assertResult(out)(SparkWordCount.process(in, 1).collect)
  }

  after {
    sc.stop()
  }
}