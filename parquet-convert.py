# CSV file name: S_rxxx_Sy_z_www.csv
# r: recording round (1-3)
# xxx: participant ID (001-465)
# y: recording session (1-2)
# z: task category (1-5)
# www: task code (VRG, PUR, VID, TEX, RAN)

DATASET_DIR = 'dataset/gazebasevr'
OUTPUT_FILE = 'dataset/gazebasevr.parquet'

if __name__ == '__main__':
    import os
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import logging

    from collections import defaultdict

    # Set logging level
    logging.basicConfig(level=logging.DEBUG)

    # create dataset list

    df_list = []

    # Loop through all the CSV files under the directory
    for root, dirs, files in os.walk(DATASET_DIR):
        for file in files:
            if file.endswith('.csv'):
                # Load CSV file
                logging.info(f"Processing {file}")
                csv_file = os.path.join(root, file)
                df = pd.read_csv(csv_file, dtype=defaultdict(lambda: "float32", n="float64"))

                # Extract metadata from the file name
                filename = os.path.splitext(file)[0]
                metadata = filename.split('_')

                df['round'] = np.int8(metadata[1][0])
                df['participant'] = np.int16(metadata[1][1:])
                df['session'] = np.int8(metadata[2][1])
                df['task'] = np.int8(metadata[3])
                # df['task_code'] = metadata[4]

                # Append to dataset list
                df_list.append(df)

    # Save dataset to parquet
    logging.info("Concatenating datasets")
    dataset = pd.concat(df_list, ignore_index=True)
    logging.info("Saving dataset to parquet")
    table = pa.Table.from_pandas(dataset)
    pq.write_table(table, OUTPUT_FILE)
    logging.info("Done")
