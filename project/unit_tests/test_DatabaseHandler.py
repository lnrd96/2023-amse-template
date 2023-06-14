import unittest
import os
from database.model import DB
from database.DatabaseHandler import DatabaseHandler


class TestDatabaseHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """ Executed once before all tests. """
        cls.handler = DatabaseHandler()
        cls.handler.initialize_database()

    def testDatabaseExists(self):
        self.assertTrue(os.path.isfile(DB.database))

    def testDatabaseIsInitialized(self):
        # table names must be in lower case
        self.assertTrue(DB.table_exists('accident'))
        self.assertTrue(DB.table_exists('participants'))
        self.assertTrue(DB.table_exists('coordinate'))
