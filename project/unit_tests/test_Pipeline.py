import os
import numpy as np
import pandas as pd
import unittest
from database.model import Accident
from Pipeline import Pipeline
from utils.CustomExceptions import RoadTypeNotFound

N_ACCIDENTS = 1146669
TEST_RESSOURCE = os.path.join('unit_tests', 'test_ressource.csv')


class TestPipeline(unittest.TestCase):
    def setUp(self):
        """ Executed respectively before every test. """
        pass

    @classmethod
    def setUpClass(cls):
        """ Executed once before all tests. """
        cls.pipeline = Pipeline()

    @classmethod
    def tearDownClass(cls):
        logfile = cls.pipeline.logfile
        os.remove(logfile)

    def testSideeffects(self):
        """ Test that preprocessing does not change the data it is given,
            but works with a copy of it. """
        df_orig = pd.DataFrame(data=np.empty((0, 0)))
        self.assertIsNot(df_orig, self.pipeline.preprocess(df_orig))

    def testLogfileExists(self):
        """ Asserts that logfile exists. """
        logfile = self.pipeline.logfile
        self.assertTrue(os.path.isfile(logfile))

    def testOSMNotARoad(self):
        """ Check that error is raised if coordinates are not a road. """
        with self.assertRaises(RoadTypeNotFound):
            # it's in the ocean
            self.pipeline.get_road_type_from_coordinate(10., 10.)

    def testDataToDB(self):
        """ Check that given dataframe gets persisted correctly to the db. """
        # load test data
        data = pd.read_csv(TEST_RESSOURCE)
        data['UJAHR'] = 666  # make sure this data is not in db yet
        # count db entries
        pre_count = Accident.select().count()
        # add data to db
        self.pipeline.data_to_db(data)
        # count db entries
        post_count = Accident.select().count()
        # leave db in clean state
        query = Accident.delete().where(Accident.year == 666)
        query.execute()
        # assert
        self.assertGreater(post_count, pre_count)

    def testOSMQuerying(self):
        """ Test that valid coordinate yields expected return value. """
        osm, psd = self.pipeline.get_road_type_from_coordinate(49.668659, 10.924400)
        self.assertIsInstance(osm, str)
        self.assertIsInstance(psd, str)

    def testDataScraping(self):
        """ Asserts that all expected data is downloaded. """
        data = self.pipeline.scrape_accident_data()
        self.assertEqual(len(data), N_ACCIDENTS)
