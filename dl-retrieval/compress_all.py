from sz3_api.pysz import SZ
import numpy as np
import pandas as pd


USECOLS = [2, 3, 4, 5, 6]

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

def compress_all(df):
    df = df.astype(np.float32)
    # ndarray = df.to_numpy()
    sz = SZ("sz3_api/libSZ3c.so")

    for col in df.columns:
        col_partition = df[col][0 : 2000000].to_numpy()
        compressed, _ = sz.compress(col_partition, 1, 0, 1e-4, 0)
        np.save(f"outputs/sz_compressed_files/{col}.npy", compressed)
    
def decompress_all():
    sz = SZ("sz3_api/libSZ3c.so") 
    cols = []
    for col in names.keys():
        col_array = np.load(f"outputs/sz_compressed_files/{col}.npy")
        cols.append(sz.decompress(col_array, (2000000,), np.float32))
            
    print("Uncompress success.")
    
if __name__ == '__main__':
    # compress_all(load_data())
    
    decompress_all()
