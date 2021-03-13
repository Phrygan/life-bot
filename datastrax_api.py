from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from util import log_event

import config

import os

MODULE = os.path.basename(__file__)[:-3]

KEYSPACE = 'users'
TABLES = {
    'users': 'users',
    'budgets': 'budgets'
}

class DataStraxApi:
    def __init__(self):
        secure_bundle_path = f'{os.path.dirname(os.path.realpath(__file__))}/secure-connect-life-bot-db.zip'
        cloud_config = {
            'secure_connect_bundle': secure_bundle_path
        }
        auth_provider = PlainTextAuthProvider(config.DATASTAX_CLIENT_ID, config.DATASTAX_CLIENT_SECRET)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        self.session = cluster.connect(KEYSPACE)
        db_version = self.session.execute('select release_version from system.local').one()
        if db_version:
            log_event(f'accessing database version: {db_version[0]}', module=MODULE)
        else:
            log_event('Could not find Version', module=MODULE)

        # create tables if not there
        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLES['users']} (
                    username text PRIMARY KEY,
                    firstname text,
                    lastname text
                );
            """
        )
        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLES['budgets']} (
                    username text PRIMARY KEY,
                    firstname text,
                    lastname text
                );
            """
        )
        log_event('Loaded users and budgets tables', module=MODULE)

    def insert_user(self, username: str, firstname: str, lastname: str):
        """
        Insert new user to database. Update user in database.

        Args:
            username (str): Username of user.
            firstname (str): User's first name.
            lastname (str): User's lastname.
        """
        self.session.execute(
            f"INSERT INTO {TABLES['users']} (username, firstname, lastname) VALUES (%s, %s, %s)",  [username, firstname, lastname]
        )
        log_event(f'Inserted (username: {username}, firstname: {firstname}, lastname: {lastname}', module=MODULE)

    def get_user(self, username: str=None):
        """
        Get user with username. Otherwise, get all users.

        Args:
            username (str, optional): username of user. Defaults to None.

        Returns:
            namedTuple: Data accessed
            iterable: Data accessed
        """
        if username:
            return self.session.execute(f"SELECT * FROM {TABLES['users']} WHERE username=%s", [username]).one()
        return self.session.execute(f"SELECT * FROM {TABLES['users']}")


    def update_user(self, username, firstname: str=None, lastname: str=None):
        """
        Update data for user with username.

        Args:
            username (str): username of user to update.
            firstname (str, optional): New firstname of user. Defaults to None.
            lastname (str, optional): New lastname of user. Defaults to None.
        """
        if firstname:
            prepared = self.session.prepare(
                f"UPDATE {TABLES['users']} SET firstname=? WHERE username=?"
            )
            self.session.execute(prepared, [firstname, username])
            log_event(f'updated username (firstname: {firstname}', module=MODULE)
        if lastname:
            prepared = self.session.prepare(
                f"UPDATE {TABLES['users']} SET lastname=? WHERE username=?"
            )
            self.session.execute(prepared, [lastname, username])
            log_event(f'updated username (lastname: {lastname})', module=MODULE)

    def delete_user(self, username: str):
        """
        Delete user with username.

        Args:
            username (str): Username of user to delete.
        """
        prepared = self.session.prepare(f"DELETE FROM {TABLES['users']} WHERE username = ?")
        self.session.execute(prepared, [username])
        log_event('Delete {username', module=MODULE)

def main():
    users = {
        'lougene': {'firstname': 'eugene', 'lastname': 'hong'},
        'neiphu': {'firstname': 'andrew', 'lastname': 'hong'}
    }
    db_api = DataStraxApi()
    db_api.update_user('neiphu', 'andrew', users['neiphu']['lastname'])
    print(db_api.get_user()[0])



if __name__ == '__main__':
    main()