# Aurum: Discovering Data in Lakes, Clouds and Databases

Aurum helps users identify relevant content among multiple data
sources that may consist of tabular files, such as CSV, and relational tables.
These may be stored in relational database management systems (RDBMS), file
systems, and they may live in cloud services, data lakes or other on-premise
repositories.

Aurum helps you find data through different interfaces. The most flexible one is
an API of primitives that can be composed to build queries that describe the
data of interest. For example, you can write a query that says "find tables that
contain a column with name 'ID' and have at least one column that looks like
an input column". You can also query with very simple primitives, such as "find
columns that contain the keyword 'caffeine'". You can also do more complex
queries, such as figuring out what tables join with a table of interest. The
idea is that the API is flexible enough to allow a wide range of use cases, and
that it works over all data you feed to the system, regardless where these live.

* [**Why do I need Aurum?**](why_aurum.md) We show you various scenarios in which Aurum has proven useful.

* [**Design Rationale**](design_rationale.md) A brief explanation of the system architecture and 
design rationale.

* [**Quick Start**](quick_start.md) A guide to setup Aurum and start running some discovery queries.

* [**Tutorial**](tutorial.md) A tutorial that walks you through the different aspects of Aurum, from how 
to write queries using the discovery API, to how to create new connectors to read data from different 
data sources to how to store data in different stores.

* [**FAQ**](faq.md) Collection of frequent questions

Aurum is a work in progress, we expect to release its first open-source version in the 4th quarter of 2018.
We are happy to accept contributions of the community. If you are interested in contributing take a look at
the [CONTRIBUTING](../CONTRIBUTING.md) and feel free to email raulcf@csail.mit.edu
We also have a code of conduct:

### Code of Conduct

Check the code of conduct for Aurum here: 

https://github.com/mitdbg/aurum-datadiscovery/blob/master/CODE_OF_CONDUCT.md

Please, report violations of the code of conduct by sending an email to
raulcf@csail.mit.edu

