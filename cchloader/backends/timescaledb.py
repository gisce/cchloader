# -*- coding: utf-8 -*-
from __future__ import absolute_import

from cchloader.backends import BaseBackend, register, urlparse
import datetime
import psycopg2
from psycopg2.extras import execute_values
import pytz


def get_as_utc_timestamp(t):
    timezone_utc = pytz.timezone("UTC")
    timezone_local = pytz.timezone("Europe/Madrid")
    return timezone_utc.normalize(timezone_local.localize(t, is_dst=False))


def get_utc_timestamp_from_datetime_and_season(local_timestamp, season):
    """
    Returns UTC timestamp from local datetime and winter/summer flag
    :param local_timestamp: datetime (no localized)
    :param season:
    :return: datetime (UTC localized)
    """
    dst = season == 1 and True or False
    timezone_utc = pytz.utc
    timezone_local = pytz.timezone("Europe/Madrid")
    utc_timestamp = (timezone_local.normalize(
        timezone_local.localize(local_timestamp, is_dst=dst))
    ).astimezone(timezone_utc)
    return utc_timestamp


class TimescaleDBBackend(BaseBackend):
    """TimescaleDB Backend
    """
    collections = ['giscedata_corbagen', 'giscedata_epfpf']

    def __init__(self, uri=None):
        if uri is None:
            uri = "timescale://localhost:5432/destral_db"
        super(TimescaleDBBackend, self).__init__(uri)

        self.uri = uri
        self.config = urlparse(self.uri)
        ts_con = " host=" + self.config['hostname'] + \
                " port=" + str(self.config['port']) + \
                " dbname=" + self.config['db'] + \
                " user=" + self.config['username'] + \
                " password=" + self.config['password']
        self.db = psycopg2.connect(ts_con)
        self.cr = self.db.cursor()
        self._columns_cache = {}

    def insert(self, document):
        self.insert_batch([document])

    def insert_batch(self, documents, page_size=1000):
        grouped_rows = {}

        for document in documents:
            for collection in document.keys():
                if collection not in self.collections:
                    continue

                cch = document.get(collection)
                if not cch:
                    continue

                cch.backend = self
                cch.collection = collection
                prepared = self._prepare_cch_document(cch)

                column_names = tuple(sorted(prepared.keys()))
                unique_fields = tuple(cch.adapter.unique_fields)
                update_fields = tuple(
                    [field for field in cch.adapter.update_fields if field in prepared]
                )

                group_key = (collection, column_names, unique_fields, update_fields)
                grouped_rows.setdefault(group_key, []).append(
                    tuple(prepared[column] for column in column_names)
                )

        grouped_iter = grouped_rows.iteritems() if hasattr(grouped_rows, 'iteritems') else grouped_rows.items()

        for group_key, rows in grouped_iter:
            collection, column_names, unique_fields, update_fields = group_key
            sql = self._build_upsert_sql(
                collection,
                column_names,
                unique_fields,
                update_fields
            )
            execute_values(self.cr, sql, rows, page_size=page_size)

        if grouped_rows:
            self.db.commit()

    def get_columns(self, collection):
        if collection in self._columns_cache:
            return self._columns_cache[collection]

        self.cr.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s", (collection, )
        )
        columns = [x[0] for x in self.cr.fetchall()]
        self._columns_cache[collection] = columns
        return columns

    def _prepare_cch_document(self, cch):
        collection = cch.collection
        document = dict(cch.backend_data)
        columns = self.get_columns(collection)
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # UTC timestamp used by GISCE
        if 'timestamp' in columns and 'local_timestamp' in document and 'season' in document:
            utc_timestamp = get_utc_timestamp_from_datetime_and_season(
                document['local_timestamp'], document['season']
            ).strftime('%Y-%m-%d %H:%M:%S')
            document['timestamp'] = utc_timestamp

        if 'created_at' in columns:
            document['created_at'] = timestamp

        if 'updated_at' in columns:
            document['updated_at'] = timestamp

        # Create and Write date
        if 'create_date' in columns:
            document['create_date'] = timestamp

        if 'write_date' in columns:
            document['write_date'] = timestamp

        # UTC timestamp used by SOM
        if 'utc_timestamp' in columns and 'datetime' in document:
            document['utc_timestamp'] = get_as_utc_timestamp(document['datetime']).strftime('%Y-%m-%d %H:%M:%S')

        if 'validated' in document and type(document['validated']) == bool:
            document['validated'] = 1 if document['validated'] else 0

        if 'datetime' in document and type(document['datetime']) == datetime.datetime:
            document['datetime'] = document['datetime'].strftime('%Y-%m-%d %H:%M:%S')

        if 'name' in document and type(document['name']) == type(u''):
            document['name'] = document['name'].encode('utf-8')

        return document

    @staticmethod
    def _build_upsert_sql(collection, column_names, unique_fields, update_fields):
        columns_sql = ', '.join(column_names)
        conflict_clause = ', '.join(unique_fields)

        if not update_fields:
            return (
                "INSERT INTO {table} ({columns}) VALUES %s "
                "ON CONFLICT ({conflict}) DO NOTHING"
            ).format(
                table=collection,
                columns=columns_sql,
                conflict=conflict_clause
            )

        set_clause = ', '.join(
            "{field} = COALESCE(EXCLUDED.{field}, {table}.{field})".format(
                field=field, table=collection
            )
            for field in update_fields
        )

        return (
            "INSERT INTO {table} ({columns}) VALUES %s "
            "ON CONFLICT ({conflict}) DO UPDATE SET {set_clause}"
        ).format(
            table=collection,
            columns=columns_sql,
            conflict=conflict_clause,
            set_clause=set_clause
        )

    def insert_cch(self, cch):
        prepared = self._prepare_cch_document(cch)
        column_names = tuple(sorted(prepared.keys()))
        unique_fields = tuple(cch.adapter.unique_fields)
        update_fields = tuple(
            [field for field in cch.adapter.update_fields if field in prepared]
        )

        sql = self._build_upsert_sql(
            cch.collection,
            column_names,
            unique_fields,
            update_fields
        )
        row = tuple(prepared[column] for column in column_names)
        execute_values(self.cr, sql, [row], page_size=1)
        self.db.commit()
        return None

    def get(self, collection, filters, fields=None):
        raise Exception("Not implemented cchloader.backend.timescale.get()")

    def disconnect(self):
        self.db = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


register("timescale", TimescaleDBBackend)
