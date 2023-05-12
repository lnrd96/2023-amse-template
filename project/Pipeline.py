import os
import re
import shutil
import logging
import zipfile
import requests
import pandas as pd

from tqdm import tqdm
from datetime import datetime
from config import DATA_SOURCE_ACCIDENTS


class Pipeline:
    """ Class to pull, preprocess (massage) and store the data. """
    def __init__(self):
        # set working directory
        os.chdir(os.path.join('2023-amse-template', 'project'))
        # set up logging
        self._setup_logging()
        
    def preprocess(self, data: pd.DataFrame):
        pass
        # select useful columns
        # datetime format conversion
        
        
        
        # dt = datetime.datetime(year, month, day, hour)
        # # Create an instance of YourModel and assign the datetime value to your_datetime_field
        # model_instance = YourModel(your_datetime_field=dt)

    def scrape_data(self) -> pd.DataFrame:
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

        # scrape metadata for all present datasets
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

        for root, dirs, files in os.walk(".", topdown=False):
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

        # read data content
        files = os.listdir(target_directory)
        data_files = [file for file in files 
                      if re.match(r'^Unfallorte\d{4}_LinRef\.(?:csv|txt)$', file)]

        # TODO
        data_frames = []
        for csv_file in data_files:
            csv_path = os.path.join(target_directory, csv_file)
            df = pd.read_csv(csv_path, delimiter=";")
            data_frames.append(df)

        # Concatenate all the data frames into a single data frame
        combined_df = pd.concat(data_frames)
        df.to_csv('unfallorte_all.csv', index=False)

        # Perform any desired operations on the combined data frame

        # Example: Print the first few rows of the combined data frame
        print(combined_df.head())

        # Remove temp dir
        os.rmdir(target_directory)  # TODO: Error: Dir not empty

    def _setup_logging(self):
        f_id = datetime.now().strftime("%m.%d.%Y_%H:%M:%S")
        filename = os.path.join('logfiles', f_id + '.log')
        logging.basicConfig(filename=filename, filemode='w',
                            format='%(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)


if __name__ == '__main__':
    # Pipeline().scrape_data()
    df = pd.read_csv('unfallorte_all.csv')
    Pipeline().preprocess(df)
