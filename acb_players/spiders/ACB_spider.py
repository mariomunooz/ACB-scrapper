import scrapy
from datetime import datetime
from acb_players.items import PlayerItem
from acb_players.items import get_id, get_url, get_player_stats
from scrapy.loader import ItemLoader



class QuotesSpider(scrapy.Spider):
    name = 'acb_spider'

    '''start_urls = [
        'https://www.acb.com/club/plantilla/id/8/temporada_id/2000',
    ]'''
    start_urls = [f'https://www.acb.com/club/plantilla/id/{id}/temporada_id/{year}' for id in range(1, 550) for year in
              range(2000, 2022)]
    start_time = datetime.now()
    print(f"Start spider at: {start_time}")



    def parse(self, response):
        team = response.xpath('//h3[@class="roboto_condensed_bold mayusculas"]/text()').get()
        pro_players = response.xpath(
            '//article[@class="caja_miembro_plantilla caja_jugador_medio_cuerpo"]/div[3]/div[1]/a/@href')
        junior_players = response.xpath(
            '//article[@class="caja_miembro_plantilla caja_jugador_cara"]/div[3]/div[1]/a/@href')
        dropped = response.xpath(
            '//table[@class="roboto defecto tabla_plantilla plantilla_bajas clasificacion tabla_ancho_completo"]/tbody/tr/td[2]/a/@href')

        # Work with items help https://www.youtube.com/watch?v=wyE4oDxScfE&t=316s&ab_channel=JohnWatsonRooney

        print(team)

        for pro_player in pro_players:
            id = get_id(pro_player)
            if id != None:
                player_url = get_url(id)
                yield response.follow(player_url, callback=self.parse_player, meta={'id': id})

        for junior in junior_players:
            id = get_id(junior)
            if id != None:
                player_url = get_url(id)
                yield response.follow(player_url, callback=self.parse_player, meta={'id': id})
        if dropped:
            for dropped_player in dropped:
                id = get_id(dropped_player)
                if id != None:
                    player_url = get_url(id)
                    yield response.follow(player_url, callback=self.parse_player, meta={'id': id})

    def parse_player(self, response):

        l = ItemLoader(item=PlayerItem(), response=response)
        l.add_xpath('name', '//h1[@class="f-l-a-100 roboto_condensed_bold mayusculas"]/text()')
        l.add_xpath('complete_name',
                    '//div[@class="datos_secundarios roboto_condensed"]/span[@class="roboto_condensed_bold"]/text()')
        l.add_value('ACB_id', response.request.url)
        l.add_xpath('birth_place',
                    '//div[@class="datos_secundarios lugar_nacimiento roboto_condensed"]/span[@class="roboto_condensed_bold"]/text()')
        l.add_xpath('image', '//div[@class="foto"]/img/@src')
        l.add_xpath('height_cm',
                    '//div[@class="datos_basicos altura roboto_condensed"]/span[@class="roboto_condensed_bold"]/text()')
        l.add_xpath('position',
                    '//div[@class="datos_basicos posicion roboto_condensed"]/span[@class="roboto_condensed_bold"]/text()')
        l.add_xpath('nationality',
                    '//div[@class="datos_secundarios nacionalidad roboto_condensed"]/span[@class="roboto_condensed_bold"]/text()')
        l.add_xpath('birth_date',
                    '//div[@class="datos_secundarios fecha_nacimiento roboto_condensed"]/span[@class="roboto_condensed_bold"]/text()')
        l.add_value('career', response)
        l.add_value('other_tables', response)
        l.add_value('date_info_obtained', datetime.now())

        player_stats_url = f'https://www.acb.com/jugador/temporada-a-temporada/id/{response.meta.get("id")}'
        player_stats = get_player_stats(player_stats_url)

        l.add_value('games_played_in_ACB', player_stats[0])
        l.add_value('minutes_played_in_ACB', player_stats[1])
        l.add_value('ACB_stats_per_season', player_stats[2])

        print('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
        yield l.load_item()



    end_time = datetime.now() - start_time
    print(f"End spider at: {end_time}")