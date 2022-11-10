# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html


import scrapy
import pandas as pd
import json as jsn
from acb_players.utils.utils import *
from datetime import datetime
from scrapy.selector import Selector
from itemloaders.processors import TakeFirst, MapCompose


def get_id(player_box):
    # Input an scrapy selector and returns the web id of the player
    link = player_box.extract()
    id = link.split('/')
    if id[1] == 'jugador':
        id = id[-1]
        id = id.split('-')
        name = f'{id[1]} {id[2]}'
        id = id[0]

        return id  # integer
    return None


def get_source_id(url):
    url = url.split('/')
    id = int(url[-1])
    '''print('id' + str(type(id)))'''
    return id


def get_url(id):
    # Input id Intiger
    return f'https://www.acb.com/jugador/trayectoria-logros/id/{id}'


def check_image(link):
    check = link.split('/')
    if check[1] == 'Images':
        return None
    else:
        check.pop(0)
        check.pop(0)
        check = '/'.join(check)
        return check


def clean_height(height):
    # gets a strin with the height in meters and returns integer the height in cm
    height = height.replace('m', '')
    height = height.replace(',', '.')
    height = float(height)
    return int(height * 100)


def clean_position(pos):
    return pos.replace('-', ' ')


def clean_date_birth(date):
    data = date.split(' ')
    return string_to_date(data[0])


def string_to_date(str):
    date_time_obj = datetime.strptime(str, '%d/%m/%Y')
    return date_time_obj


def get_career(response):
    # Access to the career player table (trayectoria deportiva) (https://www.acb.com/jugador/trayectoria-logros/id/20200836)
    # and gets every row which is stored in an a array
    # NEXT DAY: analyse the meaning of the numbers like this one (03-02 LEGA. ITA. Fabriano Basket.) in https://www.acb.com/jugador/trayectoria-logros/id/20200791
    # create method print career
    career = []

    a = ''
    i = 1

    while a != None:
        selector = f'//table[@class = "roboto defecto tabla_entidad tabla_entidad_trayectoria tabla_ancho_completo mt20"]/tbody/tr[{i}]/td/text()'
        a = response.xpath(selector).get()
        if a != None:
            career.append(a)
        i += 1

    return '|'.join(career)


def get_other_tables(response):
    # Returns all the other tables as an string where the first element is the title of the table

    table_path = f'//table[@class = "roboto defecto tabla_entidad tabla_entidad_trayectoria tabla_ancho_completo mt30"]'

    tables = response.xpath(table_path).getall()
    other_tables = []

    for i in tables:
        table_title = Selector(text=i).xpath('//thead/tr/th/div/div/text()').get()
        table_elements = Selector(text=i).xpath('//tbody/tr/td/text()').getall()

        if table_title is not None and table_elements is not None:
            table_elements.insert(0, table_title)
            table = '|'.join(table_elements)

            other_tables.append(table)
    output = '{@TABLE}'.join(other_tables)


    return output


def get_player_stats(player_stats_url):
    player_stats = pd.read_html(player_stats_url, decimal=',', thousands='.')
    try:
        player_stats = player_stats[1]
        player_stats.drop(player_stats.tail(2).index, inplace=True)

        player_stats.iloc[:, 1] = player_stats.iloc[:, 1].apply(lambda x: remove_accented_chars(x))

        games_played_in_ACB = int(player_stats.iloc[:, 2].sum())
        minutes_played_in_ACB = int(player_stats.iloc[:, 3].sum())

        json = player_stats.to_json(orient='columns')

        return games_played_in_ACB, minutes_played_in_ACB, json

    except:
        return None


def print_tables(tables):
    tables = tables.split('{@TABLE}')
    for table in tables:
        rows = table.split('|')
        for row in range(len(rows)):
            if row == 0:
                print(f'Table: {rows[row]}')
            else:
                print(f'- {rows[row]}')
        print('\n')


def json_table_to_string(json):
    data = jsn.dumps(json)
    return data


def tipus(input):
    return '\t' + str(type(input))
def date_to_str(now):
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    return dt_string


class PlayerItem(scrapy.Item):
    # Definition of the fields of the player item

    ACB_id = scrapy.Field(input_processor=MapCompose(get_source_id), output_processor=TakeFirst())
    complete_name = scrapy.Field(input_processor=MapCompose(), output_processor=TakeFirst())
    name = scrapy.Field(input_processor=MapCompose(), output_processor=TakeFirst())
    image = scrapy.Field(input_processor=MapCompose(check_image), output_processor=TakeFirst())
    height_cm = scrapy.Field(input_processor=MapCompose(clean_height), output_processor=TakeFirst())
    position = scrapy.Field(input_processor=MapCompose(clean_position), output_processor=TakeFirst())
    birth_place = scrapy.Field(input_processor=MapCompose(), output_processor=TakeFirst())
    nationality = scrapy.Field(input_processor=MapCompose(), output_processor=TakeFirst())
    birth_date = scrapy.Field(input_processor=MapCompose(clean_date_birth), output_processor=TakeFirst())
    career = scrapy.Field(input_processor=MapCompose(get_career), output_processor=TakeFirst())
    other_tables = scrapy.Field(input_processor=MapCompose(get_other_tables), output_processor=TakeFirst())

    games_played_in_ACB = scrapy.Field(input_processor=MapCompose(), output_processor=TakeFirst())
    minutes_played_in_ACB = scrapy.Field(input_processor=MapCompose(), output_processor=TakeFirst())
    ACB_stats_per_season = scrapy.Field(input_processor=MapCompose(json_table_to_string), output_processor=TakeFirst())
    date_info_obtained = scrapy.Field(input_processor=MapCompose(date_to_str), output_processor=TakeFirst())





