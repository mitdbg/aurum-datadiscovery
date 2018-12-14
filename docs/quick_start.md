## Quick Start

```shell
git clone git@github.com:mitdbg/aurum-datadiscovery.git
cd aurum-datadiscovery
```

We explain next how to configure the modules to get a barebones installation. We
do this in a series of 3 stages.

### Stage 1: Configuring DDProfiler

The profiler is built in Java (you can find it under /ddprofiler). The input are
data sources (files and tables) to analyze and the output is stored in
elasticsearch. Next, you can find instructions to build and deploy the profiler as well as
to install and configure Elasticsearch.

#### Building ddprofiler

You will need JVM 8 available in the system for this step. From the root directory go to 'ddprofiler' and do:

```shell
$> cd ddprofiler
$> bash build.sh 
```

#### Deploying Elasticsearch (tested with 6.0.0)

Download the software (note the currently supported version is 6.0.0) from:

https://www.elastic.co/products/elasticsearch

Uncompress it and then simply run from the root directory:

```shell
$> ./bin/elasticsearch
```

that will start the server in localhost:9200 by default, which is the address
you should use to configure ddprofiler as we show next.

#### Configuration of ddprofiler

There are two different ways of interacting with the profiler. One is through a
YAML file, which describes and configures the different data sources to profile.
The second way is through an interactice interface which we are currently
working on. We describe next the configuration of sources through the YAML file.

The jar file produced in the previous step accepts a number of flags, of which
the most relevant one is:

**--sources** Which accepts a path to a YAML file in which to configure the
access to the different data sources, e.g., folder with CSV files or
JDBC-compatible RDBMS.

You can find an example template file
[here](https://github.com/mitdbg/aurum-datadiscovery/blob/master/ddprofiler/src/main/resources/template.yml)
which contains documentation to explain how to use it. 

A typical usage of the profiler from the command line will look like:

Example:

```shell
$> bash run.sh --sources <path_to_sources.yml> 
```

You can consult all configuration parameters by appending **--help** or <?> as a
parameter. In particular you may be interested in changing the default
elasticsearch ports (consult *--store.http.port* and *--store.port*) in case
your installation does not use the default ones.

*Note that, although the YAML file accepts any number of data sources, at the
moment we recommend to profile one single source at a time.* Note, however, that
you can run ddprofiler as many times as necessary using a YAML with a different
data source. For example, if you want to index a repository of CSV files and a
RDBMS, you will need to run ddprofiler two times, each one configured to read
the data from each source. All data summaries will be created and stored in
elasticsearch. Only make sure to edit the YAML file appropriately each time.

### Stage 2: Building a Model

Once you have used the ddprofiler to create data summaries of all the data
sources you want, the second stage will read those and create a model. We
briefly explain next the requirements for running the model builder.

#### Requirements

*As typical with Python deployments, we recommend using a virtualenvironment (see
virtualenv) so that you can quickly wipeout the environment if you no longer
need it without affecting any system-wide dependencies.* 

Requires Python 3 (tested with 3.4, 3.5 and 3.6). Use requirements.txt to
install all the dependencies:

```shell
$> pip install -r requirements.txt
```

In a vanilla linux (debian-based) system, the following packages will need to be installed system-wide:

```shell
sudo apt-get install \
     pkg-config libpng-dev libfreetype6-dev `#(requirement of matplotlib)` \
     libblas-dev liblapack-dev `#(speeding up linear algebra operations)` \
     lib32ncurses5-dev
```

Some notes for MAC users:

If you run within a virtualenvironemtn, Matplotlib will fail due to a mismatch with the backend it wants to use. A way of fixing this is to create a file: *~/.matplotlib/matplotlibrc* and add a single line: *backend: TkAgg*.

Note you need to use elasticsearch 6.0.0 in the current version.

#### Deployment

The model builder is executed from 'networkbuildercoordinator.py', which takes
exactly one parameter, **--opath**, that expects a path to an existing folder
where you want to store the built model (in the form of Python pickle files).
For example:

```shell
$> python networkbuildercoordinator.py --opath test/testmodel/
```

Once the model is built, it will be serialized and stored in the provided path.

### Stage 3: Accessing the discovery API

The file ddapi.py is the core implementation of Aurum's API. One easy way to
access it is to deserialize a desired model and constructing an API object with
that model. The easiest way to do so is by importing init_system() function from
main. Something like:

```python
from main import init_system
api, reporting = init_system(<path_to_serialized_model>, create_reporting=False)
```

The last parameter of init_system, reporting, controls whether you want to
create a reporting API that gives you access to statistics about the model. Feel
free to say yes, but beware that it may take long times when the models are big.

## Using the Discovery API

The discovery API consists of a collection of primitives that can be combined
together to write more complex data discovery queries. Consider a scenario in
which you want to identify buildings at MIT. There is a discovery primitive to
search for specific values in a column, e.g., "Stata Center". There is another
primitive to find a column with a specific schema name, e.g., "Building Name".
If you use any of them individually, you may find a lot of values, with only a
subset being relevant, e.g., many organizations may have a table that contains a
columns named "Building Name". Combining both of them makes the purpose more
specific and therefore narrows down the qualifying data, hopefully yielding
relevant results.

To use the discovery API it is useful to know about the primitives available and
about two special objects that we use to connect the primitives together and
help you navigate the results. These objects are the **API Handler** and the
**Discovery Result Set (DRS)**. We describe them both next:

**API Handler**: This is the object that you obtain when initializing the API,
that is:

```python
api, reporting = init_system(<path_to_serialized_model>, reporting=False)
```

The API Handler gives you access to the different primitives available in the
system, so it should be the first object to inspect when learning how to use the
system.

The *Discovery Result Set (DRS)* is an object that essentially represents data
within the discovery system. For example, by creating a DRS over a table in a
storage system, we are creating a reference to that table, that can be used with
the primitives. If, for example, we want to identify columns similar to a column
*A* of interest, we will need to obtain first a reference to column *A* that we
can use in the API. That reference is the DRS, and we provide several primitives
to obtain these references. Then, if we run a similarity primitive on column
*A*, the results will also be available in a DRS object --- this is what allows
to arbitrarily combine primitives together.

DRS objects have a few functions that help to inspect their content, for
example, to print the tables they represent or the columns they represent. The
more nuanced aspect of DRS is that they have an internal state that determines
whether they represent *tables* or *columns*. This is the most important aspect
to understand about the Aurum discovery API, really. We explain it in some
detail next:

Consider the *intersection* primitive, which helps in combining two DRS by
taking their intersection, e.g., similar content *and* similar schema. It is
possible to intersect at the table (tables that appear in both DRS) or column
level (columns that appear in both of them), and this can be achieved by setting
the status of the input DRS to table or column.


### Example Discovery Queries

Soon...
