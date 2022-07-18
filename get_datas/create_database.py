import os

import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


def create_db() -> None:
    db_name = os.getenv('DB_NAME')
    try:
        #Подключаемся к существующей базе данных
        connection = psycopg2.connect(user=os.getenv('DB_USER'),
                                      password=os.getenv('DB_PASSWORD'),
                                      host="db",
                                      port="5432")
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        sql_create_database = f'create database {db_name}'
        cursor.execute(sql_create_database)
        logger.info(f"DB {db_name} created")
        connection.commit()
    except (Exception, Error) as error:
        logger.info(f"info {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()

