# -*- coding: utf-8 -*-
from __future__ import absolute_import

from cchloader.backends import BaseBackend, register, urlparse
from collections import defaultdict, OrderedDict
import datetime
import psycopg2
from psycopg2.extras import execute_values
import pytz


def get_as_utc_timestamp(t, cups=None, season=None):
    """Convert local timestamp to UTC, with Canary Islands and DST support.

    :param t: naive local datetime
    :param cups: CUPS identifier (for timezone detection)
    :param season: 0=winter, 1=summer (for DST)
    """
    timezone_utc = pytz.timezone("UTC")
    timezone_local = pytz.timezone("Europe/Madrid")
    if cups and (cups[0:7] == 'ES00316' or cups[0:7] == 'ES04016'):
        timezone_local = pytz.timezone("Atlantic/Canary")
    is_dst = season == 1
    return timezone_utc.normalize(timezone_local.localize(t, is_dst=is_dst))


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
    """TimescaleDB Backend — unified GISCE + Som Energia

    Core upsert logic from GISCE (_build_upsert_sql, _prepare_cch_document).
    Som Energia layer: DST/Canary timezone, Odoo fields, batch chunking,
    collection_prefix, expanded collections.
    """
    # TODO batch_size and collections
    batch_size = 500
    collection_prefix = 'tg_'
    collections = [
        'f1', 'p1', 'cchfact', 'cchval', 'cch_gennetabeta', 'cch_autocons',
        'giscedata_corbagen', 'giscedata_epfpf',
    ]

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

    # --- Column introspection (with cache) ---

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

    # --- GISCE core: document preparation ---

    def _prepare_cch_document(self, cch):
        """Prepare a CCH document for insertion.

        Combines GISCE core preparation with Som Energia extensions:
        - UTC timestamps (GISCE + Som with DST/Canary support)
        - Odoo audit fields (create_date, create_uid, write_date, write_uid)
        - Type conversions (validated, datetime, name encoding)
        """
        collection = cch.collection
        document = dict(cch.backend_data)
        columns = self.get_columns(collection)
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # UTC timestamp used by GISCE (from local_timestamp + season)
        if 'timestamp' in columns and 'local_timestamp' in document and 'season' in document:
            utc_timestamp = get_utc_timestamp_from_datetime_and_season(
                document['local_timestamp'], document['season']
            ).strftime('%Y-%m-%d %H:%M:%S')
            document['timestamp'] = utc_timestamp

        if 'created_at' in columns:
            document['created_at'] = timestamp

        if 'updated_at' in columns:
            document['updated_at'] = timestamp

        # Odoo audit fields (Som)
        if 'create_date' in columns:
            document['create_date'] = timestamp

        # TODO user ID
        if 'create_uid' in columns:
            document['create_uid'] = 1

        if 'write_date' in columns:
            document['write_date'] = timestamp

        if 'write_uid' in columns:
            document['write_uid'] = 1

        # UTC timestamp used by SOM (from datetime + CUPS for timezone)
        if 'utc_timestamp' in columns and 'datetime' in document:
            document['utc_timestamp'] = get_as_utc_timestamp(
                document['datetime'],
                document.get('name'),
                document.get('season')
            ).strftime('%Y-%m-%d %H:%M:%S')

        # Type conversions
        if 'validated' in document:
            if type(document['validated']) == bool:
                document['validated'] = 1 if document['validated'] else 0
            # TODO new class to use inherit for validated
            elif collection not in ('tg_cchval', 'cchval'):
                if 'validated' not in document or document['validated'] is None:
                    document['validated'] = 0
                document['validated'] = int(document['validated'])

        if 'datetime' in document and type(document['datetime']) == datetime.datetime:
            document['datetime'] = document['datetime'].strftime('%Y-%m-%d %H:%M:%S')

        if 'name' in document and type(document['name']) == type(u''):
            document['name'] = document['name'].encode('utf-8')

        return document

    # --- GISCE core: SQL generation ---

    @staticmethod
    def _build_upsert_sql(collection, column_names, unique_fields, update_fields):
        """Build PostgreSQL upsert SQL with COALESCE for safe updates."""
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

    # --- Upsert field resolution ---

    def _get_upsert_fields(self, cch, collection):
        """Get unique_fields and update_fields for a collection.

        Uses model-level definitions if available (GISCE pattern),
        otherwise falls back to Som defaults (name+utc_timestamp).
        """
        adapter = cch.adapter if hasattr(cch, 'adapter') else None

        if adapter and hasattr(adapter, 'unique_fields') and adapter.unique_fields:
            unique_fields = list(adapter.unique_fields)
        else:
            raise AttributeError('TimeScale models must have unique_fields')

        if adapter and hasattr(adapter, 'update_fields') and adapter.update_fields:
            update_fields = list(adapter.update_fields)
        else:
            update_fields = None  # Will be computed from column names

        return unique_fields, update_fields

    # --- Som layer: batch insert with chunking ---

    def insert(self, document):
        self.insert_batch([document])

    def insert_batch(self, documents, page_size=1000):
        """Insert documents grouped by collection with chunking (Som layer).

        For each document, prepares data via _prepare_cch_document (GISCE core),
        then groups and inserts in chunks using _build_upsert_sql (GISCE core).
        """
        batches_to_insert = defaultdict(list)

        for document in documents:
            for collection in self.collections:
                if collection not in document:
                    continue

                cch = document.get(collection)
                if not cch:
                    continue

                cch.backend = self
                # TODO prefix tg_
                target = self.collection_prefix + collection if not collection.startswith(self.collection_prefix) else collection
                cch.collection = target

                prepared = self._prepare_cch_document(cch)
                unique_fields, update_fields = self._get_upsert_fields(cch, target)

                batches_to_insert[target].append({
                    'data': OrderedDict(sorted(prepared.items())),
                    'unique_fields': unique_fields,
                    'update_fields': update_fields,
                })

        for collection, items in batches_to_insert.items():
            batch = []
            for item in items:
                batch.append(item)
                if len(batch) >= self.batch_size:
                    self._insert_chunk(collection, batch, page_size)
                    batch = []
            if batch:
                self._insert_chunk(collection, batch, page_size)

    def _insert_chunk(self, collection, batch, page_size=1000):
        """Insert a chunk of documents using GISCE core upsert SQL.

        Deduplicates within the batch to avoid PostgreSQL ON CONFLICT errors,
        then delegates SQL generation to _build_upsert_sql.
        """
        if not batch:
            return

        unique_fields = batch[0]['unique_fields']
        update_fields = batch[0]['update_fields']

        # Deduplicate within batch by unique key
        def make_dedup_key(item):
            data = item['data']
            return tuple(
                str(data.get(f, '')) for f in unique_fields
            )

        seen = {}
        for item in batch:
            key = make_dedup_key(item)
            seen[key] = item
        unique_items = list(seen.values())

        if not unique_items:
            return

        column_names = tuple(unique_items[0]['data'].keys())

        # Compute update_fields if not defined by model
        if update_fields is None:
            skip_fields = set(unique_fields) | {'create_date', 'create_uid'}
            update_fields = [f for f in column_names if f not in skip_fields]

        # Filter update_fields to only those present in columns
        update_fields = [f for f in update_fields if f in column_names]

        sql = self._build_upsert_sql(
            collection,
            column_names,
            tuple(unique_fields),
            tuple(update_fields)
        )

        rows = [
            tuple(item['data'].get(col) for col in column_names)
            for item in unique_items
        ]

        execute_values(self.cr, sql, rows, page_size=page_size)
        self.db.commit()

    # --- Single document insert (GISCE interface) ---

    def insert_cch(self, cch):
        """Insert a single CCH document (GISCE interface)."""
        prepared = self._prepare_cch_document(cch)
        column_names = tuple(sorted(prepared.keys()))
        unique_fields, update_fields = self._get_upsert_fields(cch, cch.collection)

        if update_fields is None:
            skip_fields = set(unique_fields) | {'create_date', 'create_uid'}
            update_fields = [f for f in column_names if f not in skip_fields]

        update_fields = [f for f in update_fields if f in prepared]

        sql = self._build_upsert_sql(
            cch.collection,
            column_names,
            tuple(unique_fields),
            tuple(update_fields)
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
