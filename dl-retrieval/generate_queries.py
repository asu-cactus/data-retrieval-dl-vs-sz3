import numpy as np
import secrets
from pathlib import Path

seed = secrets.randbits(2)

from compress import PARTITION_SIZE

N_QUERIES = 10_000
N_ROWS = 2_000_000
OUTPUT_DIR = "outputs/indices"
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


def write_indices(indices, filename):
    with open(filename, "w") as f:
        for index in indices:
            f.write(f"{index}\n")


def lower_bound(rng):
    indices = rng.integers(0, PARTITION_SIZE, size=N_QUERIES)
    write_indices(indices, f"{OUTPUT_DIR}/lower_bound.txt")


def upper_bound(rng):
    indices = np.empty(N_QUERIES, dtype=np.int32)
    for i in range(N_QUERIES):
        low = (i * PARTITION_SIZE) % N_ROWS
        high = low + PARTITION_SIZE
        indices[i] = rng.integers(low, high)
    write_indices(indices, f"{OUTPUT_DIR}/upper_bound.txt")


def row_level_randomness(rng, mean, std):
    indices = []
    length = 0
    while length < N_QUERIES:
        n_rows_this_query = max(1, round(rng.normal(mean, std)))
        if length + n_rows_this_query >= N_QUERIES:
            n_rows_this_query = N_QUERIES - length
        length += n_rows_this_query

        indices.append(np.sort(rng.integers(0, N_ROWS, size=n_rows_this_query)))
    # Concatenate all indices as a single array
    indices = np.concatenate(indices)
    write_indices(indices, f"{OUTPUT_DIR}/row_level_{mean}_{std}.txt")


def block_level_randomness(rng, mean, std):
    indices = []
    length = 0


if __name__ == "__main__":
    rng = np.random.default_rng(seed)
    # lower_bound(rng)
    # upper_bound(rng)
    row_level_randomness(rng, 50, 20)
