# dask tryout
Tryout of `dask` in a jupyter-notebook using `jupyter lab`.

## related links

**dask**:

* bag (interface used in this notebook): [https://docs.dask.org/en/latest/bag.html](https://docs.dask.org/en/latest/bag.html)
* API: [https://docs.dask.org/en/latest/api.html](https://docs.dask.org/en/latest/api.html)
* distributed client API: [https://distributed.dask.org/en/latest/api.html](https://distributed.dask.org/en/latest/api.html)
* local diagnostics: [https://docs.dask.org/en/latest/diagnostics-local.html](https://docs.dask.org/en/latest/diagnostics-local.html)
* distributed diagnostics: [https://docs.dask.org/en/latest/diagnostics-distributed.html](https://docs.dask.org/en/latest/diagnostics-distributed.html)
* repo: [https://github.com/dask/dask](https://github.com/dask/dask)
* distributed repo: [https://github.com/dask/distributed](https://github.com/dask/distributed)
* cheatsheet: [https://docs.dask.org/en/latest/cheatsheet.html](https://docs.dask.org/en/latest/cheatsheet.html)
* configuration: [https://docs.dask.org/en/latest/configuration.html](https://docs.dask.org/en/latest/configuration.html)
* journey of a task: [https://distributed.dask.org/en/latest/journey.html](https://distributed.dask.org/en/latest/journey.html)
* diagnosing performance: [https://distributed.dask.org/en/latest/diagnosing-performance.html](https://distributed.dask.org/en/latest/diagnosing-performance.html)
* examples: [https://examples.dask.org/](https://examples.dask.org/)
* examples repo: [https://github.com/dask/dask-examples](https://github.com/dask/dask-examples)


**jupyter**:

* Installation: [https://jupyterlab.readthedocs.io/en/stable/getting_started/installation.html](https://jupyterlab.readthedocs.io/en/stable/getting_started/installation.html)
* dask jupyterlab extension: [https://github.com/dask/dask-labextension](https://github.com/dask/dask-labextension)
* cell magics: [https://www.dataquest.io/blog/jupyter-notebook-tips-tricks-shortcuts/](https://www.dataquest.io/blog/jupyter-notebook-tips-tricks-shortcuts/)


**toolz**:

* API: [https://toolz.readthedocs.io/en/latest/api.html](https://toolz.readthedocs.io/en/latest/api.html)


## installation
install additionally required packages (required by dask-labextension) using
package manager:
```bash
wajig install nodejs npm
```

### create environment
#### using poetry
```bash
poetry shell
poetry update
```

#### using virtualenv
```bash
mkvirtualenv --python=$(which python3) dask_tryout
pip install -r requirements.txt
```

### install jupyter-lab extensions
In activated environment run:
```bash
jupyter labextension install dask-labextension
jupyter serverextension enable dask_labextension
```


## start jupyter lab
```bash
jupyter lab
```

### create datasets
In the left pane we open the **Dask-extension**.
Here we start a new local cluster using `+NEW`.

Inside this extension we open the additional tabs:

* `PROGRESS`
* `TASK-STREAM`
* `GRAPH`
* `PROFILE`

These tabs we arrange in our jupyter lab so we can see these tabs while we are in one of the two notebooks `pipeline.ipynb` and `dataset_creation.py`.

This ensure we can see whats going on in dask while we run cells in our notebooks.

In jupyter we open `settings.py`,  `dataset_creation.ipynb` and
`pipeline.ipynb`.

#### the settings.py
Inside settings we can adjust constants to be used inside the two jupyter
notebooks.

We can adjust the following constants (if necessary):

* the number of `partitions` to use to process the offers in the `pipeline.ipynb`
* the amount of datasets to create (variable `amount_datasets`) using
  `dataset_creation.ipynb`
* the amount of offers in each dataset (variable `records_per_partition`)
* also the datasets- and output-folder can be adjusted, but this should not
  be necessary.

#### the dataset_creation.ipynb
This notebook is used to create the fake offer datasets as required by the
`pipeline.ipynb` notebook.

#### the pipeline.ipynb
This is our main notebook. The whole tryout goes on here.