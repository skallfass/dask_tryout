"""
Constants to use inside the jupyter notebooks `dataset_creation.ipynb` and
`pipeline.ipynb`.
"""
from pathlib import Path as _Path
# inside our notebook the dask extension mangages our cluster and client
# from distributed import LocalCluster as _LocalCluster
# local_cluster = _LocalCluster(scheduler_port=8786)
# depending where your dask-scheduler is running you may have to adjust the
# value of the scheduler_url
# scheduler_url = "tcp://127.0.0.1:8786"

# amount of partitions to use in the `pipeline.ipynb`
partitions = 100

# amount of datasets to create in `dataset_creation.ipynb`
amount_datasets = 50
# amount of offers in a dataset to create in `dataset_creation.ipynb`
records_per_partition = 200


# not needed to adjust
datasets_dir = _Path("datasets")
datasets_files = datasets_dir / "*.json"
output_folder = _Path("output_data")
combined_destination = output_folder / "test.json.gz"

# ensure required folders exist
for folder in (datasets_dir, output_folder):
    folder.mkdir(exist_ok=True)
