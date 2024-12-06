import json
import os
import pandas as pd
import requests
from datetime import datetime as dt
from datetime import timezone as tz

def main(request):
    response = requests.get('https://rotogrinders.com/lineups/nhl?site=draft')
    now = dt.now(tz.utc)

    BUCKET = os.environ.get('BUCKET')
    TARGET_PATH = 'gs://{}/rotogrinders_{}.csv'.format(BUCKET, now.strftime('%Y-%m-%d_%H-%M-%S-utc'))
    # TARGET_PATH = 'rotogrinders_{}.csv'.format(now.strftime('%Y-%m-%d_%H-%M-%S-utc'))

    record = {
        'api_request': [response.url],
        'content': [response.content],
        'cookies': [json.dumps(response.cookies.get_dict())],
        'encoding': [response.encoding],
        'response_headers': [json.dumps(dict(response.headers))],
        'status_code': [response.status_code],
        'status_text': [response.reason],
        '_request_created_at': str(now)
    }

    df = pd.DataFrame(record)
    df['_request_created_at'] = pd.to_datetime(df['_request_created_at'])

    df.to_csv(
        TARGET_PATH,
        index=False
    )


if __name__ == '__main__':
    main('')