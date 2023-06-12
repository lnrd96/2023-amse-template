import pandas as pd
import os
import sys
from database.DatabaseHandler import DatabaseHandler
from Pipeline import Pipeline

""" FILE TO LAUNCH PIPELINE FROM """

if 'project' not in os.getcwd():
    os.chdir(os.path.join('2023-amse-template', 'project'))

# DatabaseHandler()._update_database()
# sys.exit(1)

# creates sqlite3 file, initializes db scheme
# DatabaseHandler().reset_database()

# instantiate pipeline
pipeline = Pipeline()

# scrapes accident data from opengeodata.nrw
data = pipeline.scrape_accident_data()

# data = pd.read_csv('/Users/leonardfischer/Uni/sem_2/de/2023-amse-template/project/unfallorte_all.csv')

# preprocess data
data = pipeline.preprocess(data)

# feeds collected data into db, queries OSM to download and assign road type
pipeline.data_to_db(data)
