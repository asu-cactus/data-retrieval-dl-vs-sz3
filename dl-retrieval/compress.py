import numpy as np
import pandas as pd
from pathlib import Path
import zstd
import time
import os

# os.environ["ZSTD_NUMTHREADS"] = "1"

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


def load_data():
    df = pd.read_csv(
        "data/star2000.csv.gz",
        header=None,
        usecols=USECOLS,
        names=list(names),
        dtype=np.float32,  # Load as float32
    )
    return df


def compress_partitions_with_sz3(df):
    df = df.astype(np.float32)
    # ndarray = df.to_numpy()
    sz = SZ("sz3_api/libSZ3c.so")
    for i in range(0, len(df), PARTITION_SIZE):
        for col in df.columns:
            col_partition = df[col][i : i + PARTITION_SIZE].to_numpy()
            compressed, _ = sz.compress(col_partition, 1, 0, 1e-4, 0)

            with open(f"outputs/sz_compressed_files/{col}-{i}-{i+len(col_partition)}.sz", "wb") as f:
                f.write(compressed)


def compress_partitions_with_zstd(df):
    df = df.astype(np.float32)
    decompression_time = 0
    # total_size = len(df)
    total_size = 2_000_000
    for i in range(0, total_size, PARTITION_SIZE):
        for col in df.columns:
            col_partition = df[col][i : i + PARTITION_SIZE].to_numpy()
            compressed = zstd.compress(col_partition)
            start_time = time.time()
            zstd.decompress(compressed)
            decompression_time += time.time() - start_time
            with open(f"outputs/zstd_compressed_files/{col}-{i}-{i+len(col_partition)}.zst", "wb") as f:
                f.write(compressed)
    print(f"Decompression time: {decompression_time*200}")


def compress_index():
    total_size = 2_173_762
    index = np.arange(total_size).astype(np.float32)

    # Partition the index and compress each partition using SZ
    sz = SZ("sz3_api/libSZ3c.so")
    for i in range(0, total_size, PARTITION_SIZE):
        index_partition = index[i : i + PARTITION_SIZE]
        compressed, _ = sz.compress(index_partition, 1, 0, 1e-4, 0)
        with open(f"outputs/sz_compressed_files/index-{i}-{i+len(index_partition)}.sz", "wb") as f:
            f.write(compressed)

# def decompress_with_zstd(num_decompression=200):
#     total_size = 2_000_000
#     total_time = 0
#     for _ in range(num_decompression):
#         for i in range(0, total_size, PARTITION_SIZE):
#             for col in names.keys():
#                 with open(f"{OUTPUT_DIR}/{col}-{i}-{i+PARTITION_SIZE}.zst", "rb") as f:
#                     compressed = f.read()
#                     start_time = time.time()
#                     zstd.ZSTD_uncompress(compressed)
#                     end_time = time.time()
#                     total_time += end_time - start_time

#     print(f"Total time: {total_time} seconds.")


if __name__ == "__main__":
    # Make output directory
    Path("outputs/sz_compressed_files").mkdir(parents=True, exist_ok=True)
    Path("outputs/zstd_compressed_files").mkdir(parents=True, exist_ok=True)

    # Compression
    df = load_data()
    compress_partitions_with_sz3(df)
    # compress_index()

    # compress_partitions_with_zstd(df)

    # decompress_with_zstd(num_decompression=200)

