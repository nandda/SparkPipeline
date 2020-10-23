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

import akka.actor.ActorSystem;
import akka.actor.CoordinatedShutdown;
import com.google.protobuf.InvalidProtocolBufferException;
import com.here.hrn.HRN;
import com.here.platform.data.client.engine.javadsl.DataEngine;
import com.here.platform.data.client.javadsl.DataClient;
import com.here.platform.data.client.javadsl.Partition;
import com.here.platform.data.client.javadsl.QueryApi;
import com.here.platform.data.client.spark.DataClientSparkContextUtils;
import com.here.platform.pipeline.PipelineContext;
import com.here.schema.rib.v2.TopologyGeometry;
import com.here.schema.rib.v2.TopologyGeometryPartitionOuterClass.TopologyGeometryPartition;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkPipelineJava {

  private static final double sampleFraction = 1.0 / 1000.0;

  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(SparkPipelineJava.class);
    JavaSparkContext sparkContext =
        new JavaSparkContext(new SparkConf().setAppName("SparkPipeline"));

    PipelineContext pipelineContext = new PipelineContext();
    HRN hereMapContent = pipelineContext.getConfig().getInputCatalogs().get("hereMapContent");
    Long hereMapContentVersion =
        pipelineContext.getJob().get().getInputCatalogs().get("hereMapContent").version();
    HRN outputCatalog = pipelineContext.getConfig().getOutputCatalog();

    Config config = ConfigFactory.empty();
    config =
        config.withValue(
            "here.platform.data-client.endpoint-locator.discovery-service-env",
            ConfigValueFactory.fromAnyRef("custom"));
    config =
        config.withValue(
            "here.platform.data-client.endpoint-locator.discovery-service-url",
            // Please use https://api-lookup.data.api.platform.hereolp.cn URL for China Environment
            // We should define a custom URL, specific to China Environment, for a discovery service
            // endpoint that allows discovering various Data Service APIs like publish, metadata,
            // query, etc.
            ConfigValueFactory.fromAnyRef("https://api-lookup.data.api.platform.here.com"));

    ActorSystem sparkActorSystem = ActorSystem.create("SparkPipelineExampleJava", config);

    try {
      JavaRDD<Partition> topologyLayerMetadata =
          queryMetadata(
                  hereMapContent,
                  hereMapContentVersion,
                  "topology-geometry",
                  sparkContext,
                  sparkActorSystem)
              .sample(true, sampleFraction);

      TopologyGeometryReader readTopologyGeometry = new TopologyGeometryReader(hereMapContent);
      JavaRDD<TopologyGeometryPartition> topologyGeometryPartition =
          topologyLayerMetadata.map(readTopologyGeometry::read);

      // gather all the topology segment lengths (in meters)
      JavaRDD<Double> roadLengths =
          topologyGeometryPartition.map(
              partition ->
                  partition
                      .getSegmentList()
                      .stream()
                      .map(TopologyGeometry.Segment::getLength)
                      .mapToDouble(Double::doubleValue)
                      .sum());

      // sum up all the lengths and extrapolate the results in m
      // divided by sampleProbability to extrapolate the value to the global sample
      Double totalMeters = roadLengths.reduce(Double::sum) / sampleFraction;

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
      logger.info(
          String.format("The estimated length of all roads in the map is: %.0fm", totalMeters));

      // In a production pipeline the output will be written to your output catalog
      logger.info(String.format("The configured output catalog is: %s", outputCatalog));

    } finally {
      CoordinatedShutdown.get(sparkActorSystem)
          .runAll(CoordinatedShutdown.unknownReason())
          .toCompletableFuture()
          .join();
    }
  }

  // Loads the list of available partitions from a catalog layer of a given version as an RDD
  private static JavaRDD<Partition> queryMetadata(
      HRN catalog,
      Long catalogVersion,
      String layerName,
      JavaSparkContext sparkContext,
      ActorSystem sparkActorSystem) {
    QueryApi query = DataClient.get(sparkActorSystem).queryApi(catalog);
    ArrayList<Partition> partitions = new ArrayList<>();
    query
        .getPartitionsAsIterator(catalogVersion, layerName, Collections.emptySet())
        .toCompletableFuture()
        .join()
        .forEachRemaining(partitions::add);
    return sparkContext.parallelize(partitions);
  }
}

//// Download and decode the data for one partition
class TopologyGeometryReader implements Serializable {
  private HRN catalog;

  TopologyGeometryReader(HRN catalog) {
    this.catalog = catalog;
  }

  TopologyGeometryPartition read(Partition partition) throws InvalidProtocolBufferException {
    return TopologyGeometryPartition.parseFrom(readRaw(partition));
  }

  private byte[] readRaw(Partition partition) {
    return DataEngine.get(DataClientSparkContextUtils.context().actorSystem())
        .readEngine(catalog)
        .getDataAsBytes(partition)
        .toCompletableFuture()
        .join();
  }
}
