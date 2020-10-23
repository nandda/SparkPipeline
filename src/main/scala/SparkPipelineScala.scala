/*
 * Copyright (c) 2018-2019 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.actor.CoordinatedShutdown.UnknownReason
import com.here.hrn.HRN
import com.here.platform.data.client.engine.scaladsl.DataEngine
import com.here.platform.data.client.scaladsl.{DataClient, Partition}
import com.here.platform.data.client.spark.DataClientSparkContextUtils
import com.here.platform.pipeline.PipelineContext
import com.here.schema.rib.v2.topology_geometry_partition.TopologyGeometryPartition
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

// Import the .awaitResult method
import com.here.platform.data.client.spark.SparkSupport._

object SparkPipelineScala {

  private val logger = LoggerFactory.getLogger(SparkPipelineScala.getClass)
  private val sampleFraction = 1.0 / 1000.0

  def main(args: Array[String]): Unit = {

    val sparkContext: SparkContext = new SparkContext(new SparkConf().setAppName("SparkPipeline"))

    val pipelineContext = new PipelineContext
    val hereMapContent = pipelineContext.config.inputCatalogs("hereMapContent")
    val hereMapContentVersion = pipelineContext.job.get.inputCatalogs("hereMapContent").version
    val outputCatalog = pipelineContext.config.outputCatalog

    val config: Config = ConfigFactory.empty
      .withValue("here.platform.data-client.endpoint-locator.discovery-service-env",
                 ConfigValueFactory.fromAnyRef("custom"))
      .withValue(
        "here.platform.data-client.endpoint-locator.discovery-service-url",
        // Please use https://api-lookup.data.api.platform.hereolp.cn URL for China Environment
        // We should define a custom URL, specific to China Environment, for a discovery service
        // endpoint that allows discovering various Data Service APIs like publish, metadata, query, etc.
        ConfigValueFactory.fromAnyRef("https://api-lookup.data.api.platform.here.com")
      )

    val appName = "SparkPipelineExampleScala"
    implicit lazy val actorSystem: ActorSystem = ActorSystem.create(appName, config)
    try {
      val topologyLayerMetadata: RDD[Partition] =
        queryMetadata(hereMapContent,
                      hereMapContentVersion,
                      "topology-geometry",
                      sparkContext,
                      actorSystem)
          .sample(withReplacement = true, sampleFraction)

      val topologyPartitions: RDD[TopologyGeometryPartition] =
        topologyLayerMetadata.map(readTopologyGeometry(hereMapContent))

      // gather all the topology segment lengths (in meters)
      val roadLengths: RDD[Double] =
        topologyPartitions.map(_.segment.map(_.length).sum)

      // sum up all the lengths and extrapolate the results in m
      // divided by sampleProbability to extrapolate the value to the global sample
      val totalMeters: Double =
        roadLengths.reduce(_ + _) / sampleFraction

      // Note that the default pipeline logging level is "warn". If
      // you are running this on Pipeline Service,
      // be sure to set the logging level accordingly
      // in order to see this message in the Splunk logs.
      // For more details, please see
      // For users using platform.here.com:
      // https://developer.here.com/olp/documentation/pipeline/topics/pipeline-logging.html
      // https://developer.here.com/olp/documentation/open-location-platform-cli/user_guide/topics/pipeline/log-commands.html
      // For users using platform.hereolp.cn:
      // https://developer.here.com/olp/cn/documentation/pipeline/topics/pipeline-logging.html
      // https://developer.here.com/olp/cn/documentation/open-location-platform-cli/user_guide/topics/pipeline/log-commands.html

      logger.info(f"The estimated length of all roads in the map is: $totalMeters%.0fm")

      // In a production pipeline the output will be written to the output catalog
      logger.info(s"The configured output catalog is: $outputCatalog")

    } finally {
      val shutdown = CoordinatedShutdown(actorSystem).run(UnknownReason)
      Await.result(shutdown, Duration.Inf)
    }
  }

  // Loads the list of available partitions from a catalog layer of a given version as an RDD
  private def queryMetadata(catalog: HRN,
                            catalogVersion: Long,
                            layerName: String,
                            sparkContext: SparkContext,
                            actorSystem: ActorSystem): RDD[Partition] = {
    val query = DataClient(actorSystem).queryApi(catalog)
    val partitions = query.getPartitionsAsIterator(catalogVersion, layerName)
    sparkContext.parallelize(partitions.awaitResult.toStream)
  }

  // Download and decode the data for one partition
  private def readTopologyGeometry(catalog: HRN)(partition: Partition) =
    TopologyGeometryPartition.parseFrom(read(catalog)(partition))

  // Download the raw data for one partition
  private def read(catalog: HRN)(partition: Partition) =
    DataEngine(DataClientSparkContextUtils.context.actorSystem)
      .readEngine(catalog)
      .getDataAsBytes(partition)
      .awaitResult()
}
