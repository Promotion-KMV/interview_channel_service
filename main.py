import os
from time import sleep
import uuid
from loguru import logger
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
import psycopg2.extras
from psycopg2 import errors
from dotenv import load_dotenv
from get_datas.get_data import get_exchange_rate, get_data_sheets
from get_datas.create_database import create_db

load_dotenv()

psycopg2.extras.register_uuid()

get_data_sheet = get_data_sheets()

class Connect_db:
    def __init__(self, connection):
        self.connection = connection

    def info_column(self, table: str) -> list:
        #Получаем список столбцов таблицы
        sql = f"SELECT column_name FROM information_schema.columns " \
              f"WHERE table_catalog = 'interview' " \
              f"and table_schema = 'public' and table_name = '{table}';"
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            list_column = cursor.fetchall()
            column = sum(list_column, [])
        cursor.close()
        return column

    def parse_data_from_sheets(self):
        #Добавляем данные для записи в postgres
        list_data = []
        for i in get_data_sheet['values']:
            if i[0] == '№':
                continue
            i.insert(0, uuid.uuid4())
            i.insert(4, int(i[3])*get_exchange_rate())
            list_data.append(tuple(i))
        yield from list_data

    def create_table(self) -> None:
        #Метод для создания таблиц postgres если необходимо
        cursor = self.connection.cursor()
        q = f"CREATE TABLE IF NOT EXISTS public.order ( \
                id uuid PRIMARY KEY, \
                num INTEGER NOT NULL, \
                orders INTEGER NOT NULL UNIQUE, \
                price_usd FLOAT, \
                price_rub FLOAT, \
                delivery TEXT)"
        cursor.execute(q)
        cursor.close()
        self.connection.commit()

    def data_saver(self) -> None:
        try:
            cursor = self.connection.cursor()
            if not self.info_column('order'):
                self.create_table()
            column = self.info_column('order')
            data = self.parse_data_from_sheets()
            column_without_quotes = '[%s]' % ', '.join(map(str, column))
            len_value = '%s,' * len(column)
            columns = column_without_quotes.replace('[', '').replace(']', '')

            cursor.execute("SELECT * FROM public.order")
            if cursor.fetchall() == []:
                query = f"INSERT INTO public.order ({columns}) " \
                        f"VALUES ({len_value[:-1]}) "
                logger.info('Записываю данные в Postgres')
                cursor.executemany(query, data)
                self.connection.commit()
                sleep(0.1)
            cursor.execute("SELECT * FROM public.order")
            cursor.close()

        except psycopg2.errors.OperationalError as ex:
            raise ex
        except psycopg2.errors.SyntaxError as ex:
            raise ex
        except psycopg2.errors.DataError as ex:
            raise ex

    def data_update(self) -> None:
        global get_data_sheet
        data = get_data_sheets()
        if data == get_data_sheet:
            logger.info('Данные актуальны')
            return
        new_data = []
        for x, y in zip(data['values'], get_data_sheet['values']):
            if x != y:
                new_data.append(x)
        logger.info('Данные в таблице измененны')
        get_data_sheet = data
        cursor = self.connection.cursor()
        logger.info('Обновляю данные в postgres')
        for i in new_data:
            query = f"UPDATE public.order " \
                    f"SET num = {i[0]}, price_usd = {i[2]}, price_rub = {int(i[2])* get_exchange_rate()}, delivery = '{str(i[3])}'  " \
                    f"WHERE orders = {i[1]}"
            cursor.execute(query)
            self.connection.commit()


def load_from_sheets(pg_conn: _connection) -> None:
    """Основной метод загрузки и обновления данных из google.sheets в Postgres"""
    postgres_saver = Connect_db(pg_conn)
    postgres_saver.data_saver()
    while True:
        #check и обновление данных
        postgres_saver.data_update()
        sleep(5)


if __name__ == '__main__':
    create_db()
    dsl = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': 'db',
        'port': 5432
    }
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sheets(pg_conn)
    pg_conn.close()
