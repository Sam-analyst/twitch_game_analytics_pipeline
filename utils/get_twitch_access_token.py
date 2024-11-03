import requests

def get_twitch_access_token(client_id, client_secret):

    url = "https://id.twitch.tv/oauth2/token"

    payload = {
        "client_id" : client_id,
        "client_secret" : client_secret,
        "grant_type" : "client_credentials"
    }

    response = requests.post(url, data=payload)

    access_token = response.json()["access_token"]

    print("here's your token:", access_token)

    return response
