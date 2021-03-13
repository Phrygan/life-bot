from util import log_event

import config

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from datetime import datetime
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
                    itemid text PRIMARY KEY,
                    total float,
                    purchases text
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
        log_event(f'Inserted User (username: {username}, firstname: {firstname}, lastname: {lastname}', module=MODULE)

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
            log_event(f'updated user {username} (firstname: {firstname}', module=MODULE)
        if lastname:
            prepared = self.session.prepare(
                f"UPDATE {TABLES['users']} SET lastname=? WHERE username=?"
            )
            self.session.execute(prepared, [lastname, username])
            log_event(f'updated user {username} (lastname: {lastname})', module=MODULE)

    def delete_user(self, username: str):
        """
        Delete user with username.

        Args:
            username (str): Username of user to delete.
        """
        prepared = self.session.prepare(f"DELETE FROM {TABLES['users']} WHERE username = ?")
        self.session.execute(prepared, [username])
        log_event('Deleted {username}', module=MODULE)

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

    def insert_item(self, itemid: str, total: float, purchases: str):
        """
        Insert new item to database. Update item in database.

        Args:
            itemid (str): ID of item.
            total (float): Total spending money.
            purchases (str): JSON string of purchases.
        """
        self.session.execute(
            f"INSERT INTO {TABLES['budgets']} (itemid, total, purchases) VALUES (%s, %s, %s)",  [itemid, total, purchases]
        )
        log_event(f'Inserted item (itemid: {itemid}, total: {total}, purchases: {purchases}', module=MODULE)
    
    def get_item(self, itemid: str=None):
        """
        Get item with itemid. Otherwise, get all item.

        Args:
            itemid (str, optional): id of item. Defaults to None.

        Returns:
            namedTuple: Data accessed
            iterable: Data accessed
        """
        if itemid:
            return self.session.execute(f"SELECT * FROM {TABLES['budgets']} WHERE itemid=%s", [itemid]).one()
        return self.session.execute(f"SELECT * FROM {TABLES['budgets']}")

    def update_item(self, itemid, total: float=-1, purchases: str=None):
        """
        Update data for item with itemid.

        Args:
            itemid (str): itemid of item to update.
            total (flat, optional): New total of item. Defaults to None.
            purchases (str, optional): New purchases of item. Defaults to None.
        """
        if total > -1:
            prepared = self.session.prepare(
                f"UPDATE {TABLES['budgets']} SET total=? WHERE itemid=?"
            )
            self.session.execute(prepared, [total, itemid])
            log_event(f'updated item {itemid} (firstname: {firstname}', module=MODULE)
        if purchases:
            prepared = self.session.prepare(
                f"UPDATE {TABLES['budgets']} SET purchases=? WHERE itemid=?"
            )
            self.session.execute(prepared, [lastname, username])
            log_event(f'updated item {itemid} (total: {total})', module=MODULE)
    
    def delete_user(self, itemid: str):
        """
        Delete item with itemid.

        Args:
            itemid (str): itemid of item to delete.
        """
        prepared = self.session.prepare(f"DELETE FROM {TABLES['budgets']} WHERE itemid = ?")
        self.session.execute(prepared, [itemid])
        log_event('Deleted {itemid}', module=MODULE)

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