from time import sleep
import uuid
import sqlite3
from loguru import logger
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
import psycopg2.extras
from psycopg2 import errors

from get_data.get_data import get_exchange_rate, get_data_sheets

psycopg2.extras.register_uuid()

get_data_sheet = get_data_sheets()
coast_rub = get_exchange_rate()

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
            i.insert(4, int(i[3])*coast_rub)
            list_data.append(tuple(i))
        yield from list_data

    def data_saver(self):
        try:
            cursor = self.connection.cursor()
            column = self.info_column('order')
            data = self.parse_data_from_sheets()
            column_without_quotes = '[%s]' % ', '.join(map(str, column))
            len_value = '%s,' * len(column)
            columns = column_without_quotes.replace('[', '').replace(']', '')
            cursor.execute("SELECT * FROM public.order")
            if cursor.fetchall() == []:
                query = f"INSERT INTO public.order ({columns})" \
                        f"VALUES ({len_value[:-1]}) "
                logger.info('Записываю данные в Postgres')
                cursor.executemany(query, data)
                self.connection.commit()
                sleep(0.1)
            cursor.execute("SELECT * FROM public.order")
            print(cursor.fetchall())
            cursor.close()
        except psycopg2.errors.OperationalError as ex:
            raise ex
        except psycopg2.errors.SyntaxError as ex:
            raise ex
        except psycopg2.errors.DataError as ex:
            raise ex


    def data_update(self):
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
                    f"SET num = {i[0]}, price_usd = {i[2]}, price_rub = {int(i[2])* coast_rub}, delivery = '{str(i[3])}'  " \
                    f"WHERE orders = {i[1]}"
            cursor.execute(query)
            self.connection.commit()
        cursor.execute(f"SELECT * FROM public.order")


def load_from_sheets(pg_conn: _connection):
    """Основной метод загрузки данных из google.sheets в Postgres"""
    postgres_saver = Connect_db(pg_conn)
    postgres_saver.data_saver()
    while True:
        #check и обновление данных
        postgres_saver.data_update()
        sleep(5)



if __name__ == '__main__':
    dsl = {
        'dbname': 'interview',
        'user': 'vlad',
        'password': '123321dc',
        # 'host': '1270.0.0.1',
        'port': 5432
    }
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sheets(pg_conn)
    pg_conn.close()
