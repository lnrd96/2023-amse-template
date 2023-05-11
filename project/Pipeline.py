import os
import re
import logging
import zipfile
import requests
import pandas as pd

from datetime import datetime
from config import DATA_SOURCE_ACCIDENTS


class Pipeline:
    """ Class to pull, preprocess (massage) and store the data. """
    def __init__(self):
        # set working directory
        os.chdir(os.path.join('2023-amse-template', 'project'))
        # set up logging
        self._setup_logging()

    def _scrape_data(self):
        logging.info(f'Scraping datasets from {DATA_SOURCE_ACCIDENTS}.')

        # tmp directory
        target_directory = '.tmp_data'
        if not os.path.exists(target_directory):
            os.makedirs(target_directory)

        # scrape actual data urls out of website content
        # such that future data will be detected too
        response = requests.get(DATA_SOURCE_ACCIDENTS)
        metadata = response.json()

        # scrape metadata for all present datasets
        for dataset in metadata["datasets"]:
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
                    target_file = os.path.join(target_directory,
                                               filename.replace('.zip', '.csv'))
                    zip_ref.extractall(target_file)
                # clean from zips
                os.remove(file_path)

        # read data content
        csv_files = os.listdir(target_directory)

        # TODO

    def _setup_logging(self):
        f_id = datetime.now().strftime("%m.%d.%Y_%H:%M:%S")
        filename = os.path.join('logfiles', f_id + '.log')
        logging.basicConfig(filename=filename, filemode='w',
                            format='%(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)


if __name__ == '__main__':
    Pipeline()._scrape_data()