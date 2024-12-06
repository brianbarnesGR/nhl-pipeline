import json
import requests

def get_contests(sport: str = None):
    sport = sport.lower()

    if sport is None:
        raise Exception('Sport not specified')
    elif sport == 'nhl':
        url = "https://www.draftkings.com/lobby/getcontests"
        payload = {
            "sport": sport
        }
        response = requests.get(url, json=payload)
        data = json.loads(response.text)

        return data
    else:
        raise Exception('Sport not recognized')