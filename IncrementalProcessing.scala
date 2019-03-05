package hcl.processing
import hcl.utils.LookupMaps
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

  class IncrementalProcessing(spark: SparkSession) extends LookupMaps {

    def processIncrementalLogic(): Unit ={
      import spark.implicits._

      val orig_table = spark.table("poc2dbuser11.pc_policyperiod_orig1")
        .select($"policyid",$"policynumber",$"billingmethod",$"segment",$"basestate",$"assignedrisk",$"termnumber",$"periodstart",$"periodend")

      val pc_policy = spark.table("pc_policy")
        .select($"id",$"packagerisk",$"originaleffectivedate",$"issuedate",$"losshistorytype")

      val pctl_losshistorytype = spark.table("pctl_losshistorytype")
        .select($"id",$"name".as("losshistorytype_name"))


      val joinDf = orig_table.as("df1")
        .join(pc_policy.as("df2"), $"df1.policyid" === $"df2.id")
        .join(pctl_losshistorytype.as("df3") , $"df2.LossHistoryType" === $"df3.id")
        .select($"df1.policyid",$"df1.policynumber",$"df1.billingmethod",$"df1.segment",$"df1.basestate",
          $"df1.assignedrisk",$"df1.termnumber",$"df1.periodstart",$"df1.periodend",$"df2.packagerisk",
          $"df2.originaleffectivedate",$"df2.issuedate",$"df3.losshistorytype_name")
        .withColumn("eff_start_date", current_timestamp())
        .withColumn("eff_end_date", lit("9999-01-01 00:00:00"))
        .withColumn("curr_ind", lit("Y"))
        .withColumn("md5_chk_sum",lit("MD1"))



      //joinDf.write.mode("overwrite").partitionBy("eff_start_date").saveAsTable("poc2dbuser11.db_policy_hub")

      val file = spark.read.option("inferSchema", "true").csv("/user/dbuser11/curation/pc_policyperiod_delta.csv")
      val file1 = file.select(file.columns.map(c => col(c).as(originLookup.getOrElse(c, c))): _*)


      file1.filter($"policyid" === "1098").show()

      val stagingMD5Df = file1.as("df1")
        .join(pc_policy.as("df2"), $"df1.policyid" === $"df2.id")
        .join(pctl_losshistorytype.as("df3") , $"df2.LossHistoryType" === $"df3.id")
        .select($"df1.policyid",$"df1.policynumber",$"df1.billingmethod",$"df1.segment",$"df1.basestate",$"df1.assignedrisk",$"df1.termnumber",$"df1.periodstart",$"df1.periodend",$"df2.packagerisk",$"df2.originaleffectivedate",$"df2.issuedate",$"df3.losshistorytype_name")
        .withColumn("staging_non_key_concat",concat_ws("_",$"policynumber",$"billingmethod",$"segment",$"basestate",$"assignedrisk",$"termnumber",$"periodstart",$"periodend",$"packagerisk",$"originaleffectivedate",$"issuedate",$"losshistorytype_name"))
        .withColumn("staging_non_key_md5", sha2($"staging_non_key_concat",256))
        .drop("staging_non_key_concat")

      val historyMD5DF = joinDf
        .select($"policyid",$"policynumber",$"billingmethod",$"segment",$"basestate",$"assignedrisk",$"termnumber",$"periodstart",$"periodend",$"packagerisk",$"originaleffectivedate",$"issuedate",$"losshistorytype_name")
        .withColumn("history_non_key_concat",concat_ws("_",$"policynumber",$"billingmethod",$"segment",$"basestate",$"assignedrisk",$"termnumber",$"periodstart",$"periodend",$"packagerisk",$"originaleffectivedate",$"issuedate",$"losshistorytype_name"))
        .withColumn("history_non_key_md5", sha2($"history_non_key_concat",256))
        .drop("history_non_key_concat")

      val incrementalLogicDF = historyMD5DF.as("df1").join(stagingMD5Df.as("df2"),
        $"df1.policyid" === $"df2.policyid","full_outer")
        .withColumn("md5Flag",
          when(($"df1.policyid" === $"df2.policyid") && ($"staging_non_key_md5" === $"history_non_key_md5"), lit("NCR"))
            .when($"df1.policyid".isNotNull && $"staging_non_key_md5".isNull, lit("NCR"))
            .when(($"df1.policyid" === $"df2.policyid") && ($"staging_non_key_md5" =!= $"history_non_key_md5"), lit("U"))
            .when($"df1.policyid".isNull && $"staging_non_key_md5".isNotNull, lit("I")))



      incrementalLogicDF.filter($"md5Flag" === "U").select($"df1.policyid".as("keycolumn1"),$"df2.policyid".as("keycolumn2"),
        $"history_non_key_md5",$"staging_non_key_md5").show(false)


      val resultDF1 = incrementalLogicDF
        .select($"df2.*",$"md5Flag")
        .filter($"md5Flag" === "I" || $"md5Flag" === "U")
        .withColumn("eff_start_date", current_timestamp())
        .withColumn("eff_end_date",lit("9999-01-01 00:00:00"))
        .withColumn("curr_ind", lit("Y"))
        .withColumn("md5_chk_sum",lit("MD1"))
        .drop("md5Flag")


      val resultDF2 = incrementalLogicDF
        .select($"df1.*",$"md5Flag")
        .filter($"md5Flag" === "NCR")
        .withColumn("eff_end_date", when($"md5Flag" === "U",current_timestamp())
          .otherwise(lit("9999-01-01 00:00:00")))
        .withColumn("eff_start_date", current_timestamp())
        .withColumn("curr_ind", lit("Y"))
        .withColumn("md5_chk_sum",lit("MD1"))
        .drop("md5Flag")


      val resultDF = resultDF1.union(resultDF2)



      resultDF.write.mode("overwrite").partitionBy("eff_start_date").saveAsTable("poc2dbuser11.db_policy_hub")


    }
  }

