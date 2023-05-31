import os
import subprocess
from database.model import DB, Accident, Participants, Coordinate
from playhouse.migrate import SqliteMigrator, migrate
from peewee import IntegerField
# from peewee import *


class DatabaseHandler():
    """ To manage the sqlite instance.
        Database design is implemented in `model.py`.
    """

    def __init__(self):
        pass

    def _update_database(self):
        """ Needed to update the database sheme during the project.
        """
        DB.connect()
        migrator = SqliteMigrator(DB)
        migrate(
            migrator.add_column('Accident', 'year', IntegerField(null=True)),
            migrator.add_column('Accident', 'month', IntegerField(null=True)),
            migrator.add_column('Accident', 'hour', IntegerField(null=True)),
            migrator.add_column('Accident', 'weekday', IntegerField(null=True))
        )
        DB.close()

    def initialize_database(self):
        """ Creates sqlite3 database instance, the file.
            Initializes the database scheme using peewee.
        """
        print('Initializing database...')
        if not os.path.isfile(DB.database):
            sqlite3 = subprocess.Popen(['sqlite3', DB.database], stdin=subprocess.PIPE)
            stdout, stderr = sqlite3.communicate(input=b';\n.schema\n.quit')
        else:
            print('Database already exists. Aborting.')
            return
        if stdout is not None:
            print(stdout.decode('utf8'))
        if stderr is not None:
            print(stderr.decode('utf8'))
        try:
            DB.connect()
            DB.create_tables([Accident, Participants, Coordinate], safe=True)
            DB.close()
        except Exception as e:
            print(f'Error creating database scheme with peeweee: {e.stderror}')
            return
        print(f'Succesfully generated database: "{DB.database}".')

    def reset_database(self):
        if os.path.exists(DB.database):
            input = 'Do you really want to delete "%s"? (y/n)' % DB.database
            if input.upper == 'Y':
                os.remove(DB.database)
                print('Deleted old database.')
            else:
                print('Aborting.')
                return
        else:
            print('Database not found. Creating new database.')
        self.initialize_database()

    def alter_scheme(self):
        input = 'Do you really want to alter the database scheme? Check the implementation first! (y/n).'
        if input.upper() == 'Y':
            self._update_database()
        else:
            print('Aborting.')
