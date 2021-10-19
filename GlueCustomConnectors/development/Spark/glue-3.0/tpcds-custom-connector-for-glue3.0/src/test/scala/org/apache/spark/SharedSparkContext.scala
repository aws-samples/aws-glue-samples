package org.apache.spark

import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkContext extends BeforeAndAfterAll {
  this: Suite =>

  protected var sparkContext: SparkContext = _
  protected val sparkConf: SparkConf = new SparkConf()

  sparkConf.set("spark.driver.host", "localhost")

  def updateSparkConf(): Unit = {}

  override def beforeAll(): Unit = {
    super.beforeAll()

    updateSparkConf()
    sparkContext = new SparkContext("local[*]", this.getClass.getName, sparkConf)
    sparkContext.setLogLevel("INFO")
  }

  override def afterAll(): Unit = {
    try {
      sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

}
