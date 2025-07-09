import os
import uuid
import requests
import json

from config import DB_API_URL, EMAIL, PASSWORD 


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
        
def get_authentication_credential():
    return DBAuthenticator(f'{DB_API_URL}/api/v1', EMAIL , PASSWORD).get_access_token()

class VisibleDbConnection:

    def __init__(self):    
        
        self.access_token = get_authentication_credential()

        self.base_url = DB_API_URL
        self.headers = {"Content-Type": "application/json",'X-Auth-Token':self.access_token} 

    @retry(requests.exceptions.RequestException, tries=3, delay=2, backoff=2)
    def get_all_projects(self):
        url = f'{self.base_url}/api/v1/projects/find/'
        resp = requests.post(url=url, data=json.dumps({'project_name':'','user_group':'jaggu'}), headers=self.headers)
        return resp.json()['projects']

    @retry(requests.exceptions.RequestException, tries=3, delay=2, backoff=2)
    def get_project_status(self, project_name):
        url = f'{self.base_url}/api/v1/projects/status/'
        resp = requests.post(url=url, data=json.dumps({'project_name':project_name,'user_group':'jaggu'}), headers=self.headers)
        return resp.json()['status']
    
    @retry(requests.exceptions.RequestException, tries=3, delay=2, backoff=2)
    def get_project_info_byKey(self, project_key):
        url = f'{self.base_url}/api/v1/projects/get_by_key/'
        resp = requests.post(url=url, data=json.dumps({'project_key':project_key}), headers=self.headers)
        return resp.json()['project']
    
    @retry(requests.exceptions.RequestException, tries=3, delay=2, backoff=2)
    def upload_to_db(self,row, project):
        url = f'{self.base_url}/api/v1/images/add/'
        headers = {"Content-Type": "application/json", 'X-Auth-Token': self.access_token}
        uid = str(uuid.uuid4())
        image = {
            'image_url': row['media_url'],
            'project_name': project,
            'image_name': uid + '.jpg',
            'image_id': uid,
            'j_url': '',
            'source': row['source'],
            'source_type': row['source_type'],
            'caption': row['caption'],
            'hashtags': row['hashtags'],
            'likes_count': row.get('like_count', 0),
            'comments_count': row['comments_count'],
            'post_url': row['permalink'],
            'media_type': row['media_type'],
            'post_username': row.get('username', ''),
            'location': {},
            'brand': row['brand'],
            'post_date': float(row['created_at'])
        }
        resp = requests.post(url=url, data=json.dumps({'image': image}), headers=headers)
        try:
            if resp.json()['message'] not in ['success', 'duplicate key']:
                print(resp.content)
        except:
            print(resp.content)