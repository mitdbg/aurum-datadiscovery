## Aurum CLI

The CLI module aims to make the Aurum workflow easy and straight forward (especially for newcomers)

Through the CLI module (see `aurum_cli.py`) one can:
* Manage data sources through the command line (or interactively), without having to edit .yml files.
* Run profile jobs by selecting specific data sources.
* Manage models (identified by names).
* Export models to other formats & databases (currently Neo4J).
* Initiate interactive discovery sessions.

To see the available commands, run
```bash
python aurum_cli.py -- --help
```
```
aurum_cli.py list-sources
aurum_cli.py add-csv
aurum_cli.py add-db
aurum_cli.py show-source
aurum_cli.py profile
aurum_cli.py build-model
aurum_cli.py list-models
aurum_cli.py export-model
aurum_cli.py clear-store
aurum_cli.py explore-model
```

Each command has it's own arguments and flags.
For example
```bash
python aurum_cli.py profile -- --help
```

```bash
Usage:       aurum_cli.py profile DATA_SOURCE_NAME
             aurum_cli.py profile --data-source-name DATA_SOURCE_NAME
```

## Example
Suppose we have a `/data/csv_data` directory contaings .csv files, we wish to explore with Aurum.

Here's how the workflow would look like.

First we save the data source (DS).
```bash
./aurum_cli.py add-csv csvs_ds /data/csv_data ,
```

We can inspect the DS we just added via
```bash
./aurum_cli.py show-source csvs_ds
```

Or list all the availale DSes to choose from.
```bash
./aurum_cli.py list-sources
```

Next, we will profile the DS we just added.
```bash
./aurum_cli.py profile csvs_ds
```

At this point we could profile additional DSes too.

Once we are done, it's time to build the model.
To make interactions easy, we tag each model with a name (e.g `my_model`).

```bash
./aurum_cli.py build-model my_model
``` 

Once it's built we can export it to other formats & databses (e.g. Neo4j).
*(This may take some time for complex models but you have a nice tqdm-powered progress bar :-) )* 

```bash
./aurum_cli.py export-model my_model neo4j localhost 7687 neo4j n304j
``` 

To run discovery queries in *interactive* mode you can just type
```bash
./aurum_cli.py explore-model my_model
```
After that, you'll be redirected into an IPython session
with the necessary already populated.

You would normally use the `api` variable of `algebra.API` type,
which you can use to run operators supported by the Aurum algebra (e.g. `api.content_serch("search term")`)

### Cleaning up
Before profiling new DSs, you may want to clear the `ElasticSearch` store.
You can do so via.
```bash
./aurum_cli.py clear-store
```  