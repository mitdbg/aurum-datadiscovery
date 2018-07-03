# Design Rationale

Aurum consists of three independent modules that work together to achieve all
the above. We explain briefly each module next:

* **DDProfiler:** The ddprofiler is in charge of reading the data from wherever it
lives (e.g., CSV files, tables, the cloud or an on-premise lake) and create a
set of summaries that succintly represent the data in a way that allows us to
discover it later. All the data summaries are stored in a store, which at the
moment is elasticsearch.

* **Model Builder:** The model builder is in charge of creating a model that can
respond to the different user queries. To build this model, the
networkbuildercoordinator.py will read the data summaries created by the
profiler from the store (elasticsearch) and will output the model to another
store, which for now is simply a pickle serialization. 

* **Front-end API:** Last, the front-end API contains the primitives and utilities
to allow users to create discovery queries. The API is configured with the path
to an existing model, which represents some underlying data. The API primitives
are then combined and query both elasticsearch and the model to answer users'
queries.

This project is a work-in-progress.