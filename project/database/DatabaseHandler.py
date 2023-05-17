import os
import subprocess
from database.model import DB, Accident, Participants, Coordinate


class DatabaseHandler():
    """ To manage the sqlite instance.
        Database design is implemented in `model.py`.
    """

    def __init__(self):
        pass

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
            os.remove(DB.database)
            print('Deleted old database.')
        else:
            print('Database not found. Creating new database.')
        self.initialize_database()

    def alter_scheme(self):
        raise NotImplementedError()
