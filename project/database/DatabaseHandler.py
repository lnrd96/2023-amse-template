import os
import subprocess
from database.model import DB, Accident, InvolvedParticipants, Coordinate
# initialize DB


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
        if 'database' in os.listdir():
            os.chdir('database')
        if '2023-amse-template' in os.listdir():
            os.chdir(os.path.join('2023-amse-template', 'project', 'database'))
        if not os.path.isfile(DB.database):
            sqlite3 = subprocess.Popen(['sqlite3', 'data.sqlite'], stdin=subprocess.PIPE)
            stdout, stderr = sqlite3.communicate(input=b';\n.quit')
        else:
            print('Database already exists. Aborting.')
            return
        if stdout is not None:
            print(stdout.decode('utf8'))
        if stderr is not None:
            print(stderr.decode('utf8'))
        try:
            DB.connect()
            DB.create_tables([Accident, InvolvedParticipants, Coordinate])
            DB.close()
        except Exception as e:
            print(f'Error creating database scheme with peeweee: {e.strerror}')
            return
        print(f'Succesfully generated database: "{DB.database}" at "{os.getcwd()}".')

    def reset_database(self):
        raise NotImplementedError()

    def alter_scheme(self):
        raise NotImplementedError()
