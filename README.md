# Read from a Catalog in a Batch Application

> **Objectives**: Estimate the total length of the road geometries of the
> HERE Map Content
>
> **Complexity**: Easy
>
> **Time to complete**: 30 min
>
> **Source code**: <a href="plain-spark-pipeline.zip" download target="_blank">Download</a>

When developing in OLP with the purpose of deploying a job in
[Pipeline Service]({{book.filtered.devGuidesUrlBase}}/pipeline-user-guide/index.html),
you can choose between two runtime environments.

* You can use *batch* to run [Spark](https://spark.apache.org)-based applications.
* You can use *stream* to run [Flink](http://flink.apache.org)-based applications.

This example demonstrates a simple Spark *batch* application that downloads
data from the
[HERE Map Content]({{book.filtered.portalUrlBase}}/data/{{book.filtered.mapContentCatalog}})
catalog `topology-geometry` layer in order to estimate the total length of the
road geometries present in the map.

The `topology-geometry` layer contains the HERE Map Content topology and
the geometry of the road segments. The spatial partitioning scheme for this
layer is `HereTile`. For more information on `HereTile` partitioning,
see [this document]({{book.filtered.devGuidesUrlBase}}/data-user-guide/shared_content/topics/olp/concepts/partitions.html#here-tile-partitioning).

Each segment also contains a `length` attribute that represents its total length
in meters.

First download the metadata for the layer that contains the list of
partitions for the layer using the `queryMetadata` and select a random
sample of about 1/1000 of the available partitions.

For each selected partition, download the related data and sum all the
lengths available in each partition. This reduces the resulting `RDD`
of doubles to a single number and sums up all the values present in the
selected tiles.

## Set up the Project in Maven

In order to develop an application that runs on pipelines with Spark, use
the `sdk-batch-bom` as the parent pom for our application:

```xml
[snippet](pom.xml#parent)
```

Adjust dependencies for Scala and Java.

```xml
[snippet](pom.xml#dependencies)
```

## Implement the Application

This application implements a [MapReduce](https://en.wikipedia.org/wiki/MapReduce)
over partitions in the `topology-geometry` layer, summing the lengths of all
road segments in each partition.

Instead of summing lengths over all the partitions, this application samples a
small subset of partitions and divides the sum of all their lengths by the
sampling rate to estimate the total length over all partitions. This
produces a reasonable estimation in a fraction of the time.

> #### Note::
> At the time of writing, there are approximately 59 million km of
> geometries in the HERE Map Content catalog.

{% codetabs name="Scala", type="scala" -%}
[snippet](./src/main/scala/SparkPipelineScala.scala)
{% language name="Java", type="java" -%}
[snippet](./src/main/java/SparkPipelineJava.java)
{% endcodetabs %}

## Configure the Pipeline

This `pipeline-config.conf` file declares the HRN for HERE Map Content
as the `heremapcontent` input catalog for the pipeline, as well
as an HRN for the output catalog. This pipeline does not
write an output catalog, so the output HRN is just a dummy value.

In a production pipeline, the output HRN would point to
an existing catalog to which the app and/or sharing group
has write permissions. For more on managing these
permissions, see
[this document]({{book.filtered.devGuidesUrlBase}}/access-control/user-guide/index.html).

```
[snippet](./pipeline-config.conf)
```

This `pipeline-job.conf` file declares the versions of
the `heremapcontent` input and the dummy output.

```
[snippet](./pipeline-job.conf)
```

## Run the Application

{% codetabs name="Run Scala", type="sh" -%}
mvn compile exec:java \
    -Dexec.mainClass=SparkPipelineScala \
    -Dpipeline-config.file=pipeline-config.conf \
    -Dpipeline-job.file=pipeline-job.conf \
    -Dspark.master=local[*]
{% language name="Run Java", type="sh" -%}
mvn compile exec:java \
    -Dexec.mainClass=SparkPipelineJava \
    -Dpipeline-config.file=pipeline-config.conf \
    -Dpipeline-job.file=pipeline-job.conf \
    -Dspark.master=local[*]
{% endcodetabs %}

## Further Information

From here, you can try running this job on Pipeline Service.
For more information about how to do this, refer to the
[Pipeline Commands]({{book.filtered.devGuidesUrlBase}}/open-location-platform-cli/user_guide/topics/pipeline-commands.html)
in the OLP CLI Developer Guide.

Remove the call to `sample` on the metadata and the scaling of the final result
in the `totalMeters` variable. This transforms this program from an estimator
to a parallel program that sums up all the values in the catalog in a few minutes.

You can also try to publish the information on the total number of km into a
`text\plain` layer with `generic` partitioning and a single partition with
catalog information. See the tutorial on [Creating a Catalog](../create-catalog/README.md)
and the [Command Line Interface Developer Guide]({{book.filtered.devGuidesUrlBase}}/open-location-platform-cli/user_guide/topics/data/catalog-commands.html)
for more information on creating catalogs and configuring layers.
