import numpy as np
from pathlib import Path

from compress import PARTITION_SIZE

N_QUERIES = 10_000
N_ROWS = 2_000_000
OUTPUT_DIR = "outputs/indices"
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


def write_indices(indices, filename):
    with open(filename, "w") as f:
        for index in indices:
            f.write(f"{index}\n")


def lower_bound():
    indices = np.random.randint(PARTITION_SIZE, size=N_QUERIES)
    write_indices(indices, f"{OUTPUT_DIR}/lower_bound.txt")


def upper_bound():
    indices = np.empty(N_QUERIES, dtype=np.int32)
    for i in range(N_QUERIES):
        start = (i * PARTITION_SIZE) % N_ROWS
        end = start + PARTITION_SIZE
        indices[i] = np.random.randint(start, end)
    write_indices(indices, f"{OUTPUT_DIR}/upper_bound.txt")


if __name__ == "__main__":
    lower_bound()
    upper_bound()
