# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import mysql.connector

class AcbPlayersPipeline:
    def __init__(self):
        self.con = mysql.connector.connect(
            host = 'localhost',
            user = 'root',
            passwd = 'mininet',
            database = 'acbplayers'
        )
        self.cur = self.con.cursor()
        self.create_table()

    def create_table(self):
        self.cur.execute("""CREATE TABLE IF NOT EXISTS acb_players(
        ACB_id INTEGER PRIMARY KEY,
        complete_name TEXT,
        name TEXT,
        image TEXT,
        height_cm INTEGER,
        position TEXT,
        birth_place TEXT,
        nationality TEXT,
        birth_date TEXT,
        career TEXT,
        other_tables TEXT,
        games_played_in_ACB INTEGER,
        minutes_played_in_ACB INTEGER,
        ACB_stats_per_season TEXT,
        date_info_obtained TEXT)
        """)
    def check_other_tables(self, item):
        print('CHECKING OTHER TABLES')
        try:
            print('Trying to access other tables')
            print(item['other_tables'])
            print(type(item['other_tables']))
            return item['other_tables']
        except:
            print('returned value is None')
            return 'null'

    def process_item(self, item, spider):

        self.cur.execute("""insert ignore into acb_players values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (
        item.get('ACB_id'),
        item.get('complete_name'),
        item.get('name'),
        item.get('image'),
        item.get('height_cm'),
        item.get('position'),
        item.get('birth_place'),
        item.get('nationality'),
        item.get('birth_date'),
        item.get('career'),
        item.get('other_tables'),
        item.get('games_played_in_ACB'),
        item.get('minutes_played_in_ACB'),
        item.get('ACB_stats_per_season'),
        item.get('date_info_obtained')))
        self.con.commit()
        return item