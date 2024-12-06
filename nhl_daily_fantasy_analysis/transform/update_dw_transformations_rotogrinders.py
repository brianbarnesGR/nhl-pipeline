import os
import pandas as pd
import re
# import pandas_gbq
import gcsfs

from bs4 import BeautifulSoup as bs
from datetime import datetime as dt, timezone as tz

def unpack_rotogrinders_nhl_html(html:str, api_request_timestamp) -> list:
    soup = bs(html, features='html.parser')
    players = []

    for game in soup.select('.game-card'):
        date_text = soup.select_one('.page-body .container-header .muted') \
                        .text \
                        .strip() \
                        .strip('\\n')
        
        game_date = re.sub(r' \([^)]*\)', '', date_text)
                
        # 0=Away, 1=Home     
        for i in [0, 1]:
            team_abbrev = game.select('.team-nameplate-title')[i]['data-abbr']
            
            if i == 0:
                opponent = game.select('.team-nameplate-title')[1]['data-abbr']
            else:
                opponent = game.select('.team-nameplate-title')[0]['data-abbr']

            # Get goalie data
            g = game.select('.lineup-card .lineup-card-pitcher .player-nameplate')[i]
        
            goalie = {
                'team': team_abbrev,
                'opponent': opponent,
                'name': g.select_one('.player-nameplate-name').contents[0],
                'position': 'G',
                'line': 'Line 1',
                'status': 'Active',
                'pp_line': None,
                'salary': g['data-salary'],
                'projected_points': None,
                'projected_owned': None,
                '_api_request_timstamp': api_request_timestamp,
                '_game_date': game_date
            }

            players.append(goalie)

            lineup_card = game.select('.lineup-card')[i]
            skaters = lineup_card.select('.lineup-card-player .player-nameplate')

            skater_i = 1
            for skater in skaters:
                if skater_i in [1, 2, 3, 13, 14]:
                   line = 1
                elif skater_i in [4, 5, 6, 15, 16]:
                   line = 2
                elif skater_i in [7, 8, 9, 17, 18]:
                   line = 3
                elif skater_i in [10, 11, 12]:
                   line = 4
                else:
                   line = None

                skater_i = skater_i + 1 if skater_i < 15 else 1

                if len(skater.select('.small.bold.red')) > 0:
                    pp_line = skater.select('.small.bold.red')[0].contents[0]
                else:
                    pp_line = None

                if skater['data-salary'] == '':
                   skater_name = skaters[11].select_one('.player-nameplate-info').contents[1].text
                else:
                   skater_name = skater.select_one('.player-nameplate-name').contents[0]

                player = {
                    'team': team_abbrev,
                    'opponent': opponent,
                    'name': skater_name,
                    'position': skater['data-position'],
                    'line': 'Line {}'.format(line),
                    'status': 'Inactive' if ('disabled' in skater['class']) else 'Active',
                    'pp_line': pp_line,
                    'salary': None if skater['data-salary'] == '' else skater['data-salary'],
                    'projected_points': None,
                    'projected_owned': None,
                    '_api_request_timstamp': api_request_timestamp,
                    '_game_date': game_date
                }

                players.append(player)

    return players

# def load_bigquery(df, project_id, dataset, table):
#     pandas_gbq.to_gbq(df, '{}.{}'.format(dataset, table), project_id=project_id, if_exists='append')

def main(event, context):
    # Import environment variables
    PROJECT = os.environ.get('PROJECT')
    BUCKET = os.environ.get('BUCKET')
    DATASET = os.environ.get('DATASET')

    filename = event['name']
  
    try:
        df = pd.read_csv('gs://{}/{}'.format(BUCKET, filename), dtype=str)
  
        # Specify the BigQuery table to update
        if 'rotogrinders' in event['name']:
            table = 'rotogrinders'
        elif 'moneypuck' in event['name']:
            table = 'moneypuck'
        else:
            return -1
    
        players = []

        for index, row in df.iterrows():
            html = row['content']
            request_time = row['_request_created_at']
            players += unpack_rotogrinders_nhl_html(html, request_time)  

        export = pd.DataFrame(players)
        export['_api_request_timestamp'] = pd.to_datetime(export._api_request_timstamp, utc=True)
        export['_game_date'] = pd.to_datetime(export._game_date)

        load_bigquery(export, PROJECT, DATASET, table)
    
    except Exception as e:
        print('Failed to write to dw_transformations.rotogrinders: {}'.format(str(e)))
        print('Event ID: {}'.format(context.event_id))
        print('Event type: {}'.format(context.event_type))
        print('Bucket: {}'.format(event['bucket']))
        print('File: {}'.format(event['name']))
        print('Metageneration: {}'.format(event['metageneration']))
        print('Created: {}'.format(event['timeCreated']))
        print('Updated: {}'.format(event['updated']))

if __name__ == '__main__':
    df = pd.read_csv('rotogrinders_2024-12-04_18-00-23-utc.csv')
    players = []

    for index, row in df.iterrows():
        html = row['content']
        request_time = row['_request_created_at']
        players += unpack_rotogrinders_nhl_html(html, request_time)  

    export = pd.DataFrame(players)
    export['_api_request_timestamp'] = pd.to_datetime(export._api_request_timstamp, utc=True)
    export['_game_date'] = pd.to_datetime(export._game_date)
    export.to_csv('rotogrinders.csv', index=False)