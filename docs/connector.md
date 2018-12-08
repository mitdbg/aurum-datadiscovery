# Reading and Profiling new data sources with Aurum

Aurum's profiler is in charge of reading and profiling external data sources.  A
*data source* is a system that stores *relations*. For example, a Postgres
database is a data source, and so it is a folder in a machine, if it stores a
bunch of CSV files.  Each data source is accessed differently. To read the
relations from a RDBMS, such as Postgres, you may need to use a JDBC connector,
while to read the relations from the CSV folder a IO API may suffice. Aurum's
profiler can read from multiple data sources, provided there is a *Source*
implementation for them. By default, Aurum has *Source* implementations for a
bunch of different data sources. This guide explains how to add new ones.

This is a work in progress. Suggestions on how to smooth the process of adding
new data sources are accepted and appreciated. There are a few moving parts
aimed to aid developers who are following this process.

*1- Let the system know there is a new source available.

Go to
[sources.SourceType](https://github.com/mitdbg/aurum-datadiscovery/blob/master/ddprofiler/src/main/java/sources/SourceType.java)
, and add a new Enum entry to the list. Choose a name that does not exist yet
and that describes well the new data source. As you see, each data source
declared there takes a parameter, which is the file that contains its
configuration:

*2- Implement a configuration file for the new data source.

Each data source has different configuration parameters that a user must input
to be able to read the source. For example, a CSV file may contain a
'separator', while a JDBC connection may have a 'port'. Examples of how a user
of Aurum will input these parameters to read from a data source 
are in [this example YAML
file](https://github.com/mitdbg/aurum-datadiscovery/blob/master/ddprofiler/src/main/java/sources).

To read and parse correctly that YAML file, a developer writes a *SourceConfig*
configuration. Examples of those are in the
[sources.config](https://github.com/mitdbg/aurum-datadiscovery/blob/master/ddprofiler/src/main/java/sources/config/)
package. 

The newly implemented SourceConfig file is then passed as a parameter to the new
type when adding the entry in the SourceType class (step 1 above).

*3- Implement the Source interface

Finally, the developer of the new *data source* must implement the
[*Source*](https://github.com/mitdbg/aurum-datadiscovery/blob/master/ddprofiler/src/main/java/sources/Source.java)
interface and place it in the sources.implementations package, where there are
some existing implementations one can use as a guiding example.


