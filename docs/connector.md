# Guide to Create New Connectors

Aurum's profiler is in charge of reading and profiling external data sources.
Each data source is accessed differently, and this connection is encapsulated in
a *connector*. There are existing connectors to read from CSV files in file
systems and from relations in JDBC-compatible databases. We say Aurum has
*connectors* to *sources* In this guide we walk
through the process of creating new connectors, should you need to connect to a
new data source.

*The process is being streamlines, and suggestions accepted (or better, PRs), but
here's a current guide to build your own connector.* 

The steps are roughtly:

*1- Let the system know there is a new source available.

When using Aurum, a user must configure the sources from which to read data.
That is done with a YAML file. You can find an example here:

[here](https://github.com/mitdbg/aurum-datadiscovery/blob/master/ddprofiler/src/main/resources/template.yml)

The first step is to make Aurum recognize there is a new source. For that, first
declare the new source name in
[core/SourceType][https://github.com/mitdbg/aurum-datadiscovery/blob/master/ddprofiler/src/main/core/SourceType.java].
Then, write the condition for the YAML parser to detect the new source. You can
do that
[here][https://github.com/mitdbg/aurum-datadiscovery/blob/master/ddprofiler/src/main/core/config/sources/YAMLParser.java] 

as you see, that function takes a SourceConfig.class file, which is specific to
the new source. The new step is then to create such file.

*2- Decide the configuration parameters of the new source.

Each source may have different configuration parameters. For example, a CSV file
may contain a 'separator', while a JDBC connection may have a 'port'. Examples
of these configuration parameters are in the YAML template above. To use these
configuration parameters, the profiler must have them in a *SourceConfig* file,
which is essentially a class that implements the SourceConfig.java interface,
accessible
[here][https://github.com/mitdbg/aurum-datadiscovery/blob/master/ddprofiler/src/main/core/config/sources/SourceConfig.java]

and you can find different examples in *core.config.sources*. In this step you
need to create a new SourceConfig which is specific to your source. One good way
of naming the new file is to concatenate the name of the new source, say,
*NewDB*, with *SourceConfig*, e.g., *NewDBSourceConfig* and place it in the same
package with the others.

*3- Time to pass the source configuration to the Source handler.

In Main.java, you will see examples of how to process each of the different
existing sources. Again, you will need to create a condition to detect the new
source, and inside, create a new class, specific to the new source, which knows
how to configure a connection to it. Specifically, you will need to create a
class that implements *sources.Source.java*. You can look into that package to
find other examples.

The *processSource* of the Source interface will take care of: i) connecting
to the source (e.g, creating a stream to a file, or a connection to a database),
read the different datasets inside the source (files or tables) and create a
*ProfileTask* for each one of it.

*4- Final step is to create the *Connector*, which will, in general, be a class
variable of the ProfileTask. This is so that the profiler knows how to access
the source-specific connector. As you'll see in the examples, these Connectors
implement a series of functions that determine how data must be read from the
external data sources, and how it's delivered to the system.


