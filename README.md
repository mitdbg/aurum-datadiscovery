##Aurum: Data Discovery at Scale

Aurum consists of three layers that can be run independently. L1 is the *data
discovery profiler*. Its purpose is to read raw data from CSV files or databases
and create *profiles*---concise representations of the information---that are
stored in a configured store (elasticsearch for the time being). L2 reads the
profiles from the store and creates a model that represents the relationships
between them. This model is then used by L3, the *discovery API*, to answer
queries posed by users.

Next there is a brief WiP description of how to build, configure and deploy the
three layers.

###Deploying L1

The profiler is built in Java (you can find it under /ddprofiler) and is meant to be
deployed standalone. The input to L1 are data sources to analyze, the output is
stored in a store. Elasticsearch is the store supported at the moment. Next, you
can find instructions to build and deploy L1 as well as to install and configure
Elasticsearch.

####Building L1

Just go to 'ddprofiler' (visible from the project root) and do:

$> ./gradlew clean fatJar

Note that the gradle wrapper (gradlew) does not require you to install any
software; it will handle the entire build process without help.

After that command, L1 is built into a single jar file that you can find in
ddprofiler/build/libs/ddprofiler.jar

####Deploying Elasticsearch (tested with 2.3.3)

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

For offline mode, this is a typical configuration:

$> java -jar <path_to_ddprofiler.jar> --db.name <name> --execution.mode 1
--sources.folder.path <path>

*--db.name* is used internally to identify the folder of data

*--execution.mode* is used to indicate whether L1 will work online (0), offline,
reading from files (1) or offline, reading from a DB (2).

*--sources.folder.path* when execution mode is 1, this option indicates the folder
with the files to process (CSV files only for now).

You can consult all configuration parameters by appending *--help* or <?> as a
parameter. In particular you may be interested in changing the default
elasticsearch ports (consult *--store.http.port* and *--store.port*) in case
your installation does not use the default ones.

###Running L2

####Requirements

Requires Python 3 (tested with 3.4.2, 3.5.0 and 3.5.1). Use requirements.txt to install all the dependencies:

$> pip install -r requirements.txt 

In a vanilla linux (debian-based) system, the following packages will need to be installed system-wide:
- sudo apt-get install pkg-config libpng-dev libfreetype6-dev (requirement of matplotlib)
- sudo apt-get install libblas-dev liblapack-dev (speeding up linear algebra operations)
- sudo apt-get install lib32ncurses5-dev

Some notes for MAC users:

There have been some problems with uWSGI. One quick workaround is to 
remove the version contraint explained in the requirements.txt file. 

There have been problems when using any other elasticsearch version than 2.3.3.

#### Deployment

The core implementation of L2 is the file networkbuildercoordinator.py
This file accepts one parameters *--opath* that expects a path to a folder where
you want to store the built model. For example:

$> python networkbuildercoordinatory.py --opath test/testmodel/

Once the model is built, it will be serialized into the provided path.

### Running L3

The file ddapi.py is the core implementation of Aurum's API. One easy way to
access it is to deserialize a desired model and constructing an API object with
that model. The easiest way to do so is by importing init_system() function from
main. Something like:

$> from main import init_system
$> api, reporting = init_system(<path_to_serialized_model>, reporting=False)

The last parameter of init_system, reporting, controls whether you want to
create a reporting API that gives you access to statistics about the model. Feel
free to say yes, but beware that it may take long times when the models are big.

