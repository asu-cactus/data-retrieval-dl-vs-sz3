import numpy as np
import pandas as pd
from pathlib import Path

# Remember to include "sz3_api" folder in LD_LIBRARY_PATH by running:
# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/your/custom/path/
from sz3_api.pysz import SZ

USECOLS = [2, 3, 4, 5, 6]
PARTITION_SIZE = 200_000  # Total size is 2,173,762
names = {
    "dst": np.int32,
    "hist": np.int32,
    "enumber": np.int32,
    "etime": np.float32,
    "rnumber": np.int32,
}
OUTPUT_DIR = "outputs/compressed_files"


def load_data():
    df = pd.read_csv(
        "data/star2000.csv.gz",
        header=None,
        usecols=USECOLS,
        names=list(names),
        dtype=np.float32,  # Load as float32
    )
    return df


def compress_partitions(df):
    df = df.astype(np.float32)
    # ndarray = df.to_numpy()
    sz = SZ("libSZ3c.so")
    for i in range(0, len(df), PARTITION_SIZE):
        for col in df.columns:
            col_partition = df[col][i : i + PARTITION_SIZE].to_numpy()
            compressed, _ = sz.compress(col_partition, 1, 0, 1e-4, 0)

            with open(f"{OUTPUT_DIR}/{col}-{i}-{i+len(col_partition)}.sz", "wb") as f:
                f.write(compressed)


def compress_index():
    total_size = 2_173_762
    index = np.arange(total_size).astype(np.float32)

    # Partition the index and compress each partition using SZ
    sz = SZ("libSZ3c.so")
    for i in range(0, total_size, PARTITION_SIZE):
        index_partition = index[i : i + PARTITION_SIZE]
        compressed, _ = sz.compress(index_partition, 1, 0, 1e-4, 0)
        with open(f"{OUTPUT_DIR}/index-{i}-{i+len(index_partition)}.sz", "wb") as f:
            f.write(compressed)


if __name__ == "__main__":
    # Make output directory
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # Compression
    df = load_data()
    compress_partitions(df)
    compress_index()
