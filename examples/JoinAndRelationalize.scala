/*
 * Copyright 2016-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Amazon Software License (the "License"). You may not use
 * this file except in compliance with the License. A copy of the License is
 * located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import com.amazonaws.services.glue.util.JsonOptions
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import org.apache.spark.SparkContext

object JoinAndRelationalize {
  def main(sysArgs: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)

    // catalog: database and table names
    val dbName = "legislators"
    val tblPersons = "persons_json"
    val tblMembership = "memberships_json"
    val tblOrganization = "organizations_json"

    // output s3 and temp directories
    val outputHistoryDir = "s3://glue-sample-target/output-dir/legislator_history"
    val outputLgSingleDir = "s3://glue-sample-target/output-dir/legislator_single"
    val outputLgPartitionedDir = "s3://glue-sample-target/output-dir/legislator_part"
    val redshiftTmpDir = "s3://glue-sample-target/temp-dir/"

    // Create dynamic frames from the source tables
    val persons: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = tblPersons).getDynamicFrame()
    val memberships: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = tblMembership).getDynamicFrame()
    var orgs: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = tblOrganization).getDynamicFrame()

    // Keep the fields we need and rename some.
    orgs = orgs.dropFields(Seq("other_names", "identifiers")).renameField("id", "org_id").renameField("name", "org_name")

    // Join the frames to create history
    val personMemberships = persons.join(keys1 = Seq("id"), keys2 = Seq("person_id"), frame2 = memberships)

    val lHistory = orgs.join(keys1 = Seq("org_id"), keys2 = Seq("organization_id"), frame2 = personMemberships)
      .dropFields(Seq("person_id", "org_id"))

    // ---- Write out the history ----

    // Write out the dynamic frame into parquet in "legislator_history" directory
    println("Writing to /legislator_history ...")
    lHistory.printSchema()

    glueContext.getSinkWithFormat(connectionType = "s3", options = JsonOptions(Map("path" -> outputHistoryDir)),
      format = "parquet", transformationContext = "").writeDynamicFrame(lHistory)

    // Write out a single file to directory "legislator_single"
    val sHistory: DynamicFrame = lHistory.repartition(1)

    println("Writing to /legislator_single ...")
    glueContext.getSinkWithFormat(connectionType = "s3", options = JsonOptions(Map("path" -> outputLgSingleDir)),
      format = "parquet", transformationContext = "").writeDynamicFrame(lHistory)

    // Convert to data frame, write to directory "legislator_part", partitioned by (separate) Senate and House.
    println("Writing to /legislator_part, partitioned by Senate and House ...")

    glueContext.getSinkWithFormat(connectionType = "s3",
      options = JsonOptions(Map("path" -> outputLgSingleDir, "partitionKeys" -> List("org_name"))),
      format = "parquet", transformationContext = "").writeDynamicFrame(lHistory)

    // ---- Write out to relational databases ----
    println("Converting to flat tables ...")
    val frames: Seq[DynamicFrame] = lHistory.relationalize(rootTableName = "hist_root",
      stagingPath = redshiftTmpDir, JsonOptions.empty)

    frames.foreach { frame =>
      val options = JsonOptions(Map("dbtable" -> frame.getName(), "database" -> "dev"))
      glueContext.getJDBCSink(catalogConnection = "test-redshift-3", options = options, redshiftTmpDir = redshiftTmpDir)
        .writeDynamicFrame(frame)
    }
  }
}
