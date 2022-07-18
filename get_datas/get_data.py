import os
from time import sleep

import requests
from dotenv import load_dotenv
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
from get_datas.config import CREDENTIALS_FILE
from loguru import logger

load_dotenv()
# Авторизуемся и получаем service — экземпляр доступа к API
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    CREDENTIALS_FILE,
    ['https://www.googleapis.com/auth/spreadsheets',
     'https://www.googleapis.com/auth/drive'])
httpAuth = credentials.authorize(httplib2.Http())
service = apiclient.discovery.build('sheets', 'v4', http = httpAuth)

# Получаем данные из google sheets
def get_data_sheets() -> dict:
    logger.info('Получаю данные из таблицы')
    values = service.spreadsheets().values().get(
        spreadsheetId=os.getenv('SPREADSHEET_ID'),
        range='A:Z',
        majorDimension='ROWS'
    ).execute()
    return values

# Получаем курс доллара
url_cb = 'https://www.cbr-xml-daily.ru/daily_json.js'
get_exchange_rate = lambda: requests.get(url_cb).json()['Valute']['USD']['Value']

get_data_google_sheets = get_data_sheets()