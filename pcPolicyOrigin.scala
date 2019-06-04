package hcl.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import hcl.utils.LookupMaps

class pcPolicyOrigin(spark: SparkSession) extends LookupMaps {

  def createPcPolicyOrigin(): Unit = {
    val file = spark.read.option("inferSchema", "true").csv("/user/dbuser11/curation/pc_policyperiod_orig.csv")

    val policyOrigin = file.select(file.columns.map(c => col(c).as(pc_account_orig_map.getOrElse(c, c))): _*)

    policyOrigin
      .withColumn("eff_start_date", current_timestamp())

    println("pc_policyOrigin")
    policyOrigin.printSchema()
    policyOrigin.show()

     }


}
