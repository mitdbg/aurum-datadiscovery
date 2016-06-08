##Aurum: Data Discovery at Scale

The prototype consists of three layers that cooperate with each other. L1 is the
profiler, in charge of extracting profiling information from multiple data
sources. L2 is the network builder in charge of organizing the profiling
information into a network of knowledge and inferring hidden relationships among
the data. L3 is the query processing layer, in charge of querying and ranking
results that answer users queries.

###Deploying L1

The high-performance profiler is built in Java (/ddprofiler) and is meant to be
deployed standalone. The input to L1 are data sources to analyze, the output is
stored in a store. Elasticsearch is the store supported at the moment. Next, you
can find instructions to build and deploy L1 as well as to install and configure
Elasticsearch.

####Building L1

Just go to 'ddprofiler' (visible from the project root) and do:

$> ./gradlew clean fatJar

L1 is built into a single jar file that you can find in
ddprofiler/build/libs/ddprofiler.jar

####Deployinst Elasticsearch (tested with 2.3)

Download the software from:

https://www.elastic.co/products/elasticsearch

Uncompress it and then simply run from the root directory:

$> ./bin/elasticsearch

that will start the server in localhost:9200 by default, which is the address
you should use to configure L1

####Configuration of L1

L1 can run in online mode, in which it receives commands and data sources to
analyze through a REST API, or offline mode, in which you can indicate the
folder with data sources to analyze.

You can configure the mode as well as a bunch of other configuration parameters
through command line parameters. You can consult all configuration parameters by
appending <help> or <?> as a parameter.

###Running L2

soon...

###Running L3

soon...

