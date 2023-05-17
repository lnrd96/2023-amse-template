from database.DatabaseHandler import DatabaseHandler
from Pipeline import Pipeline

""" FILE TO LAUNCH PIPELINE FROM """

# creates sqlite3 file, initializes db scheme
DatabaseHandler().initialize_database()

# instantiate pipeline
pipeline = Pipeline()

# scrapes accident data from opengeodata.nrw
data = pipeline.scrape_accident_data()

# preprocess data
data = pipeline.preprocess(data)

# feeds collected data into db, queries OSM to download and assign road type
pipeline.data_to_db(data)
