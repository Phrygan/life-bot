from util import log_event

import config

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from datetime import datetime
import os

MODULE = os.path.basename(__file__)[:-3]

KEYSPACE = 'users'
TABLE_NAMES = {
    'users': 'users',
    'budgets': 'budgets'
}
TABLE_FORMAT = {
    TABLE_NAMES['users']: [
        'username',
        'firstname',
        'lastname'
    ],
    TABLE_NAMES['budgets']: [
        'itemid',
        'total',
        'purchases'
    ]
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
            CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE_NAMES['users']} (
                    username text PRIMARY KEY,
                    firstname text,
                    lastname text
                );
            """
        )
        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE_NAMES['budgets']} (
                    itemid text PRIMARY KEY,
                    total float,
                    purchases text
                );
            """
        )
        log_event('Loaded users and budgets tables', module=MODULE)

    def insert(self, table: str, primary_key: str, data):
        """
        Insert data into table.

        Args:
            table (str): name of table to insert to.
            primary_key (str): Primary Key of data.
            data (dict): Data to insert.

        Returns:
            ResultSet: Response of execution of insertion.
        """
        response = self.session.execute(
            f"INSERT INTO {TABLE_NAMES[table]} ({', '.join(data)}) VALUES ({'%s, ' * 2 + '%s'})", list(data.values())
        )
        updated_data_string = ', '.join(f'{name}: {value}' for name, value in data.items())
        log_event(f"Inserted {table[:-1]} ({updated_data_string})", module=MODULE)
        return response

    def get(self, table: str, primary_key: str=None):
        """
        Access data with primary key, or access all data

        Args:
            primary_key (str, optional): Primary key of data to access. Defaults to None.
            table (str): Name of table to access.

        Returns:
            Row (namedTuple): Data accessed
            ResultSet: Data accessed
        """
        if primary_key:
            return self.session.execute(f"SELECT * FROM {TABLE_NAMES[table]} WHERE {TABLE_FORMAT[table][0]}=%s", [primary_key]).one()
        return self.session.execute(f"SELECT * FROM {TABLE_NAMES[table]}")
    
    def delete(self, table: str, primary_key: str):
        """
        Delete row with primary key.

        Args:
            table (str): Name of table to change.
            primary_key (str): Primary key of row to delete.

        Returns:
            ResultSet: Response of execution of deletion.
        """
        prepared = self.session.prepare(f"DELETE FROM {TABLE_NAMES['users']} WHERE {TABLE_FORMAT[table][0]}=?")
        response = self.session.execute(prepared, [primary_key])
        log_event(f"Deleted {TABLE_FORMAT[table][0]}: {primary_key}", module=MODULE)
        return response

    @staticmethod
    def itemid(date: datetime, item_name: str):
        """
        Convet datetime object and string to itemid

        Args:
            date (datetime): Datetime accurate to month
            item_name (str): Name of Item

        Returns:
            itemid (str): itemid
        """
        return f'{date.month}-{date.year}:{item_name}'
    
    @staticmethod
    def parse_itemid(itemid: str):
        """
        Convert itemid to datetime object and name string.

        Args:
            itemid (str): itemid

        Returns:
            tuple: datetime object accurate to month and item name.
        """
        if '-' not in itemid or ':' not in itemid:
            return None
        date_string, item_name = itemid.split(':')
        month, year = date_string.split('-')
        return datetime(int(year), int(month), 1), item_name

def main():
    users = {
        'lougene': {'firstname': 'eugene', 'lastname': 'hong'},
        'neiphu': {'firstname': 'andrew', 'lastname': 'hong'}
    }
    db_api = DataStraxApi()
    insert_response = db_api.insert(TABLE_NAMES['users'], 'neiphu', {'username': 'neiphu', 'firstname': users['neiphu']['firstname'], 'lastname': users['neiphu']['lastname']})
    db_api.insert(TABLE_NAMES['users'], 'lougene', {'username': 'lougene', 'firstname': users['lougene']['firstname'], 'lastname': users['lougene']['lastname']})
    get_response = db_api.get(TABLE_NAMES['users'], primary_key='lougene')
    delete_response = db_api.delete(TABLE_NAMES['users'], 'lougene')
    get_all_response = db_api.get(TABLE_NAMES['users'])
    print(f"insert response ({type(insert_response)}): {insert_response}\nget response ({type(get_response)}): {get_response}\n"
    f"delete response({type(delete_response)}): {delete_response}\nget all response ({type(get_all_response)}): {get_all_response}")

if __name__ == '__main__':
    main()