import os
import re
import shutil
import logging
import zipfile
import time
import requests
import pandas as pd

from database.model import Accident, Participants, Coordinate
from utils.CustomExceptions import RoadTypeNotFount
from typing import Tuple
from decimal import Decimal
from tqdm import tqdm
from datetime import datetime
from config import DATA_SOURCE_ACCIDENTS, DATA_SOURCE_ROAD_TYPES


class Pipeline:
    def __init__(self):
        """ Class to pull, preprocess (massage) and store the data. """
        # set working directory
        os.chdir(os.path.join('2023-amse-template', 'project'))
        # set up logging
        self._setup_logging()

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Preprocessing the scraped data.

        Args:
            df (pd.DataFrame): Data to preprocess.

        Returns:
            pd.dataFrame: The preprocessed data.
        """
        return df

    def data_to_db(self, df: pd.DataFrame):
        """ Feeds the data present in the given dataframe into the database.
            Uses `get_road_type_from_coordinate` to add road type info to
            accidents.

        Args:
            df (pd.DataFrame): The data scraped by `scrape_accident_data`.
                               Optionally preprocessed by `preprocess`.
        """
        n_fails, n_success = 0, 0
        for frame in tqdm(df.itertuples(index=False)):
            frame = frame._asdict()
            participants, _ = \
                Participants.get_or_create(predestrian=bool(frame['IstFuss']),
                                           truck=bool(frame['IstGkfz']),
                                           motorcycle=bool(frame['IstKrad']),
                                           bike=bool(frame['IstRad']),
                                           other=bool(frame['IstSonstig']))
            coordinate, _ = \
                Coordinate.get_or_create(utm_zone='32N',
                                         utm_x=Decimal(frame['LINREFX'].replace(',', '.')),
                                         utm_y=Decimal(frame['LINREFY'].replace(',', '.')),
                                         wsg_long=Decimal(frame['XGCSWGS84'].replace(',', '.')),
                                         wsg_lat=Decimal(frame['YGCSWGS84'].replace(',', '.')))
            try:
                osm_type, parsed_type = self.get_road_type_from_coordinate(latitude=coordinate.wsg_lat,
                                                                           longitude=coordinate.wsg_long)
            except RoadTypeNotFount as e:
                logging.info(e)
                n_fails += 1
                participants.delete_instance()
                coordinate.delete_instance()

            Accident.get_or_create(road_state=frame['STRZUSTAND'],
                                   severeness=frame['UKATEGORIE'] - 1,  # such that all $\in [0,2]$.
                                   lighting_conditions=frame['ULICHTVERH'],
                                   road_type_osm=osm_type,
                                   road_type_parsed=parsed_type,
                                   involved=participants,
                                   location=coordinate)
            n_success += 1
            if n_success % 500 == 0:
                logging.info(f'Created {n_success} entries out of {len(df)}.')

        logging.info('_' * 50 + '\n' + f'Added {n_success} entries to the database. {n_fails} potential entries were'
                     'not added due to query issues.' + '\n' + '_' * 50)

    def get_road_type_from_coordinate(self, latitude: float, longitude: float) -> Tuple[str, str]:
        """ Queries Open Street Map (OSM) using Nominatim's Reverse Geocoding to
            get the road type of the coordinate.

        Args:
            latidude (float): Coordinate component.
            longitude (float): Coordinate component.

        Raises:
            RoadTypeNotFount: In case the API query failed or the given
                              coordinate is not on a road.

        Returns:
            Tuple[str, str]: Two possible road types for the coordinate.
                             First one is OSM's road type.
                             See "https://wiki.openstreetmap.org/wiki/Key:highway#Highway".
                             Second one is road type parsed out of road name.
                             It will be 'undefined' in case road name was not available.
        """
        # format API URL
        url = DATA_SOURCE_ROAD_TYPES.format(latidude=latitude, longidude=longitude)
        # query API
        while True:
            try:
                response = requests.get(url)
                logging.info('Connection reset by peer. Retrying in 100 seconds.')
                break
            except ConnectionError:
                time.sleep(100)
        if response.status_code == 200:
            # get response as JSON
            data = response.json()
            # sanity checks
            if data['osm_type'] != 'way' or data['category'] != 'highway':
                raise RoadTypeNotFount('Not a road.')
            # the open street map road type
            osm_road_type = data['type']
            # semantically parse the road name to assign a road type
            try:
                road_name = data['address']['road']
            except KeyError:
                logging.info(f'Road name not available by OSM.')
                return osm_road_type, 'undefined'
                
            if re.match(r'^A\s*\d+$', road_name):
                extracted_road_type = 'Highway'  # Autobahn
            elif re.match(r'^B\s*\d+$', road_name):
                extracted_road_type = 'National Road'  # Bundesstrasse
            elif re.match(r'^L\s*\d+$', road_name):
                # Take me home to the plaaace I beloong
                extracted_road_type = 'Country Road'  # Landstrasse
            elif re.match(r'^[A-Z]+\s\d+$', road_name):
                extracted_road_type = 'Distict Road'  # Kreisstrasse
            else:
                extracted_road_type = 'Residential Road'  # Wohngebiet
        else:
            raise RoadTypeNotFount('API query failed.')
            logging.info(f'Error querying road type: HTML Response {response.status_code}.')
        # return result
        return osm_road_type, extracted_road_type

    def scrape_accident_data(self) -> pd.DataFrame:
        """ Downloads and unifies all datasets from the URI
            defined in the config file.

        Returns:
            pd.DataFrame: The merged datasets as a single dataframe.
        """
        logging.info(f'Scraping datasets from {DATA_SOURCE_ACCIDENTS}.')

        # tmp directory
        target_directory = '.tmp_data'
        if not os.path.exists(target_directory):
            os.makedirs(target_directory)

        # scrape actual data urls out of website content
        # such that future data will be detected too
        response = requests.get(DATA_SOURCE_ACCIDENTS)
        metadata = response.json()

        # scrape metadata for all present datasets and download them
        for dataset in tqdm(metadata["datasets"]):
            for file in dataset["files"]:
                # filter for correct files
                if not re.match(r"^Unfallorte.*CSV\.zip$", file['name']):
                    continue
                file_url = DATA_SOURCE_ACCIDENTS + '/' + file["name"]
                filename = os.path.basename(file["name"])
                file_path = os.path.join(target_directory, filename)
                logging.info(f'Found data file at URI "{file_url}". Downloading...')
                # download
                response = requests.get(file_url)
                with open(file_path, "wb") as file:
                    file.write(response.content)
                # unzip
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    new_data_dir = os.path.join(target_directory,
                                                filename.replace('.zip', ''))
                    zip_ref.extractall(new_data_dir)
                    new_file_name = os.path.basename(new_data_dir) + '.csv'
                    try:
                        shutil.move(os.path.join(new_data_dir, new_file_name), target_directory)
                    except Exception as e:
                        logging.error(f'Error moving file. {e.args[0]}')

        # filter for useful files
        for root, dirs, files in os.walk('.tmp_data', topdown=False):
            if len(files) == 2:
                for file in files:
                    if file.endswith('.csv') or file.endswith('.txt') and 'LinRef' in file:
                        try:
                            shutil.move(os.path.join(root, file), target_directory)
                        except Exception as e:
                            logging.error(f'Error moving file. {e.args[0]}')
            elif len(files) == 3:
                for file in files:
                    if file.endswith('.csv') and 'LinRef' in file:
                        shutil.move(os.path.join(root, file), target_directory)

        # read data content into pandas dataframe
        files = os.listdir(target_directory)
        data_files = [file for file in files
                      if re.match(r'^Unfallorte\d{4}_LinRef\.(?:csv|txt)$', file)]
        data_frames = []
        for csv_file in data_files:
            csv_path = os.path.join(target_directory, csv_file)
            df = pd.read_csv(csv_path, delimiter=";")
            data_frames.append(df)

        # concatenate all the data frames into a single data frame
        combined_df = pd.concat(data_frames)
        combined_df.to_csv('unfallorte_all.csv', index=False)
        return combined_df

        # remove artifacts
        shutil.rmtree(target_directory, ignore_errors=True)

    def _setup_logging(self):
        f_id = datetime.now().strftime("%m.%d.%Y_%H:%M:%S")
        filename = os.path.join('logfiles', f_id + '.log')
        logging.basicConfig(filename=filename, filemode='w',
                            format='%(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)
        print(f'Logfile is at "{os.path.abspath(filename)}"')
