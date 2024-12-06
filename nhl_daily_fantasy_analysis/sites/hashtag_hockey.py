import io
import json
import os
import pandas as pd
import re
import requests

from bs4 import BeautifulSoup
from datetime import datetime as dt
from datetime import timezone as tz

def main(request):
  BUCKET = os.environ.get('BUCKET')
  now = dt.now(tz.utc)

  # Connect to the player projection page
  response = requests.get('https://hashtaghockey.com/fantasy-hockey-projections')
  soup = BeautifulSoup(response.text, features='html.parser')

  # Parse the season the data pertains to
  season = re.search('[0-9]+-[0-9]+', soup.find('title').text)
  df = pd.read_html(io.StringIO(response.text))[2]
  df['season'] = season.group()

  # Parse the date the data was last updated
  soup.find('small').text
  date = re.search('[0-9]{1,2} [a-zA-Z]+ 20[0-9]{2}', soup.find('small').text)
  df['update_date'] = pd.to_datetime(date.group())

  # Add metadata to the column
  df['api_request'] = response.url
  df['content'] = response.content
  df['cookies'] = json.dumps(response.cookies.get_dict())
  df['encoding'] = response.encoding
  df['response_headers'] = json.dumps(dict(response.headers))
  df['status_code'] = response.status_code
  df['status_text'] = response.reason
  df['_request_created_at']: str(now)

  # Save the dataframe to object storage as a csv
  df.to_csv('gs://{}/hashtag_hockey_projections_{}.csv'.format(BUCKET, pd.to_datetime(date.group()).strftime('%Y-%m-%d')), index=False)
