import requests
import json
import os


class DBAuthenticator:
    def __init__(self, login_url, username, password):
        self.login_url = login_url
        self.username = username
        self.password = password

    def validate_session(self, access_token):
        url = f'{self.login_url}/session/verify_token/'
        headers = {'X-Auth-Token':access_token}
        resp = requests.get(url, headers=headers)
        return resp.status_code == 200

    def get_access_token(self, token_path='sec_token.json'):
        access_token = None
        if os.path.exists(token_path):
            with open(token_path, 'r') as f:
                access_token = json.load(f)['access-token']
            if not self.validate_session(access_token):
                access_token = None

        if access_token is None:
            url = f'{self.login_url}/session/sign_in/'
            headers = {"Content-Type": "application/json"}
            data = json.dumps({'email':self.username, 'password':self.password})
            resp = requests.post(url = url, data= data, headers = headers)
            token = resp.headers['access-token']
            with open(token_path, 'w') as f:
                json.dump({'access-token':token}, f)

        with open(token_path, 'r') as f:
            return json.load(f)['access-token']

