# -*- coding: utf-8 -*-
"""
Compatibility tests for unified cchloader (GISCE + Som Energia).

Tests cover:
- TimescaleDB backend: _build_upsert_sql, _prepare_cch_document, _get_upsert_fields
- UTC timestamp functions: get_as_utc_timestamp (DST, Canary Islands)
- Batch insert flow: chunking, deduplication
- Backend base: insert_batch with error handling
- Backend registry: urlparse Python 2/3 compatibility
"""
from __future__ import absolute_import

import datetime
import unittest

import pytz

from cchloader.backends.timescaledb import (
    TimescaleDBBackend,
    get_as_utc_timestamp,
    get_utc_timestamp_from_datetime_and_season,
    _DEFAULT_UNIQUE_FIELDS,
    _DEFAULT_UNIQUE_FALLBACK,
)
from cchloader.backends import urlparse
from cchloader.backends.base import BaseBackend


# ===========================================================================
# Helper mocks
# ===========================================================================

class MockAdapter(object):
    """Adapter mock with unique_fields and update_fields (GISCE pattern)."""
    def __init__(self, unique_fields=None, update_fields=None):
        self.unique_fields = unique_fields or []
        self.update_fields = update_fields or []
        self.backend = None

    def _invoke_processors(self, *args, **kwargs):
        return self._data

    def set_data(self, data):
        self._data = data


class MockAdapterNoUpsert(object):
    """Adapter mock without unique_fields/update_fields (Som fallback pattern)."""
    def __init__(self):
        self.backend = None

    def _invoke_processors(self, *args, **kwargs):
        return self._data

    def set_data(self, data):
        self._data = data


class MockDocument(object):
    """Lightweight document mock for testing."""
    def __init__(self, data, adapter):
        self.data = data
        self.adapter = adapter
        self.backend = None
        self.collection = None

    @property
    def backend_data(self):
        return dict(self.data)


# ===========================================================================
# Tests: _build_upsert_sql (GISCE core)
# ===========================================================================

class TestBuildUpsertSql(unittest.TestCase):

    def test_upsert_with_update_fields(self):
        """GISCE pattern: model defines unique_fields + update_fields."""
        sql = TimescaleDBBackend._build_upsert_sql(
            'giscedata_epfpf',
            ('ai', 'ae', 'name', 'timestamp'),
            ('name', 'timestamp'),
            ('ai', 'ae')
        )
        self.assertIn('INSERT INTO giscedata_epfpf', sql)
        self.assertIn('ON CONFLICT (name, timestamp)', sql)
        self.assertIn('COALESCE(EXCLUDED.ai, giscedata_epfpf.ai)', sql)
        self.assertIn('COALESCE(EXCLUDED.ae, giscedata_epfpf.ae)', sql)

    def test_upsert_do_nothing(self):
        """When no update_fields, should DO NOTHING on conflict."""
        sql = TimescaleDBBackend._build_upsert_sql(
            'some_table',
            ('name', 'timestamp'),
            ('name', 'timestamp'),
            ()
        )
        self.assertIn('DO NOTHING', sql)
        self.assertNotIn('DO UPDATE', sql)

    def test_upsert_with_single_unique_field(self):
        """Single unique field in ON CONFLICT."""
        sql = TimescaleDBBackend._build_upsert_sql(
            'tg_f1',
            ('ai', 'name', 'utc_timestamp'),
            ('name',),
            ('ai',)
        )
        self.assertIn('ON CONFLICT (name)', sql)

    def test_upsert_with_three_unique_fields(self):
        """Three unique fields (like tg_p1 with type)."""
        sql = TimescaleDBBackend._build_upsert_sql(
            'tg_p1',
            ('ai', 'name', 'type', 'utc_timestamp'),
            ('name', 'utc_timestamp', 'type'),
            ('ai',)
        )
        self.assertIn('ON CONFLICT (name, utc_timestamp, type)', sql)

    def test_upsert_sql_values_placeholder(self):
        """SQL should contain VALUES %s placeholder for execute_values."""
        sql = TimescaleDBBackend._build_upsert_sql(
            'tg_f1',
            ('name', 'ai'),
            ('name',),
            ('ai',)
        )
        self.assertIn('VALUES %s', sql)

    def test_upsert_coalesce_references_table(self):
        """COALESCE should reference the table name for fallback value."""
        sql = TimescaleDBBackend._build_upsert_sql(
            'tg_cchfact',
            ('name', 'utc_timestamp', 'value'),
            ('name', 'utc_timestamp'),
            ('value',)
        )
        self.assertIn('COALESCE(EXCLUDED.value, tg_cchfact.value)', sql)


# ===========================================================================
# Tests: get_as_utc_timestamp (Som DST + Canary support)
# ===========================================================================

class TestGetAsUtcTimestamp(unittest.TestCase):

    def test_madrid_winter(self):
        """Madrid winter: UTC+1."""
        t = datetime.datetime(2024, 1, 15, 10, 0, 0)
        result = get_as_utc_timestamp(t, cups='ES0031408433164001SV0F', season=0)
        self.assertEqual(result.hour, 9)
        self.assertEqual(result.tzinfo, pytz.UTC)

    def test_madrid_summer(self):
        """Madrid summer: UTC+2."""
        t = datetime.datetime(2024, 7, 15, 10, 0, 0)
        result = get_as_utc_timestamp(t, cups='ES0031408433164001SV0F', season=1)
        self.assertEqual(result.hour, 8)

    def test_canary_islands_winter(self):
        """Canary Islands (ES00316*): UTC+0 in winter."""
        t = datetime.datetime(2024, 1, 15, 10, 0, 0)
        result = get_as_utc_timestamp(t, cups='ES00316XXXXXXXXXXXXX', season=0)
        self.assertEqual(result.hour, 10)

    def test_canary_islands_summer(self):
        """Canary Islands summer: UTC+1."""
        t = datetime.datetime(2024, 7, 15, 10, 0, 0)
        result = get_as_utc_timestamp(t, cups='ES00316XXXXXXXXXXXXX', season=1)
        self.assertEqual(result.hour, 9)

    def test_canary_islands_es04016(self):
        """Canary Islands alternative prefix ES04016."""
        t = datetime.datetime(2024, 1, 15, 12, 0, 0)
        result = get_as_utc_timestamp(t, cups='ES04016XXXXXXXXXXXXX', season=0)
        self.assertEqual(result.hour, 12)  # UTC+0 in winter

    def test_no_cups_defaults_madrid(self):
        """Without CUPS, defaults to Madrid timezone."""
        t = datetime.datetime(2024, 1, 15, 10, 0, 0)
        result = get_as_utc_timestamp(t, cups=None, season=0)
        self.assertEqual(result.hour, 9)  # Madrid winter: UTC+1

    def test_season_none_defaults_winter(self):
        """Season None should default to winter (is_dst=False)."""
        t = datetime.datetime(2024, 1, 15, 10, 0, 0)
        result = get_as_utc_timestamp(t, cups='ES0031408433164001SV0F', season=None)
        # is_dst = (None == 1) = False → winter
        self.assertEqual(result.hour, 9)


class TestGetUtcTimestampFromDatetimeAndSeason(unittest.TestCase):

    def test_winter(self):
        """Season 0 (winter): Madrid UTC+1."""
        t = datetime.datetime(2024, 1, 15, 10, 0, 0)
        result = get_utc_timestamp_from_datetime_and_season(t, 0)
        self.assertEqual(result.hour, 9)

    def test_summer(self):
        """Season 1 (summer): Madrid UTC+2."""
        t = datetime.datetime(2024, 7, 15, 10, 0, 0)
        result = get_utc_timestamp_from_datetime_and_season(t, 1)
        self.assertEqual(result.hour, 8)


# ===========================================================================
# Tests: _get_upsert_fields (unified resolution)
# ===========================================================================

class TestGetUpsertFields(unittest.TestCase):

    def setUp(self):
        # Create a backend instance without DB connection for testing
        self.backend = TimescaleDBBackend.__new__(TimescaleDBBackend)
        self.backend._columns_cache = {}

    def test_adapter_with_unique_fields(self):
        """GISCE pattern: adapter defines unique_fields + update_fields."""
        adapter = MockAdapter(
            unique_fields=['timestamp', 'name', 'type'],
            update_fields=['ai', 'ae']
        )
        doc = MockDocument({}, adapter)
        unique, update = self.backend._get_upsert_fields(doc, 'giscedata_epfpf')
        self.assertEqual(unique, ['timestamp', 'name', 'type'])
        self.assertEqual(update, ['ai', 'ae'])

    def test_adapter_without_unique_fields(self):
        """Som pattern: adapter without unique_fields → default fallback."""
        adapter = MockAdapterNoUpsert()
        doc = MockDocument({}, adapter)
        unique, update = self.backend._get_upsert_fields(doc, 'tg_f1')
        self.assertEqual(unique, _DEFAULT_UNIQUE_FALLBACK)
        self.assertIsNone(update)

    def test_tg_p1_default_includes_type(self):
        """tg_p1 collection has 'type' in unique fields by default."""
        adapter = MockAdapterNoUpsert()
        doc = MockDocument({}, adapter)
        unique, update = self.backend._get_upsert_fields(doc, 'tg_p1')
        self.assertEqual(unique, ['name', 'utc_timestamp', 'type'])

    def test_adapter_with_empty_unique_fields(self):
        """Empty unique_fields list should trigger fallback."""
        adapter = MockAdapter(unique_fields=[], update_fields=[])
        doc = MockDocument({}, adapter)
        unique, update = self.backend._get_upsert_fields(doc, 'tg_f1')
        self.assertEqual(unique, _DEFAULT_UNIQUE_FALLBACK)

    def test_unknown_collection_uses_default_fallback(self):
        """Unknown collection uses default name+utc_timestamp."""
        adapter = MockAdapterNoUpsert()
        doc = MockDocument({}, adapter)
        unique, _ = self.backend._get_upsert_fields(doc, 'tg_unknown')
        self.assertEqual(unique, ['name', 'utc_timestamp'])


# ===========================================================================
# Tests: _insert_chunk deduplication
# ===========================================================================

class TestInsertChunkDedup(unittest.TestCase):

    def setUp(self):
        self.backend = TimescaleDBBackend.__new__(TimescaleDBBackend)
        self.backend._columns_cache = {}
        self.executed_sql = []
        self.executed_rows = []

        # Mock cursor and db
        class MockCursor:
            pass
        class MockDb:
            def commit(inner_self):
                pass
        self.backend.cr = MockCursor()
        self.backend.db = MockDb()

    def test_deduplication_by_unique_key(self):
        """Duplicate entries by unique key should be deduplicated."""
        from collections import OrderedDict

        batch = [
            {
                'data': OrderedDict([('ai', 100), ('name', 'CUPS1'), ('utc_timestamp', '2024-01-01 00:00:00')]),
                'unique_fields': ['name', 'utc_timestamp'],
                'update_fields': None,
            },
            {
                'data': OrderedDict([('ai', 200), ('name', 'CUPS1'), ('utc_timestamp', '2024-01-01 00:00:00')]),
                'unique_fields': ['name', 'utc_timestamp'],
                'update_fields': None,
            },
            {
                'data': OrderedDict([('ai', 300), ('name', 'CUPS2'), ('utc_timestamp', '2024-01-01 00:00:00')]),
                'unique_fields': ['name', 'utc_timestamp'],
                'update_fields': None,
            },
        ]

        # Track what execute_values receives
        captured_rows = []
        captured_sql = []

        import cchloader.backends.timescaledb as tsmod
        original_ev = tsmod.execute_values

        def mock_execute_values(cr, sql, rows, page_size=1000):
            captured_sql.append(sql)
            captured_rows.extend(rows)

        tsmod.execute_values = mock_execute_values
        try:
            self.backend._insert_chunk('tg_f1', batch)
        finally:
            tsmod.execute_values = original_ev

        # Should have 2 unique rows (CUPS1 last-wins + CUPS2)
        self.assertEqual(len(captured_rows), 2)
        # Should have generated valid SQL
        self.assertEqual(len(captured_sql), 1)
        self.assertIn('INSERT INTO tg_f1', captured_sql[0])
        self.assertIn('ON CONFLICT', captured_sql[0])


# ===========================================================================
# Tests: insert_batch flow (Som layer)
# ===========================================================================

class TestInsertBatchFlow(unittest.TestCase):

    def test_collection_prefix_applied(self):
        """Collections should get tg_ prefix when inserted."""
        backend = TimescaleDBBackend.__new__(TimescaleDBBackend)
        backend._columns_cache = {'tg_f1': ['name', 'ai', 'utc_timestamp', 'validated', 'datetime']}
        backend.collection_prefix = 'tg_'
        backend.collections = ['f1']
        backend.batch_size = 500

        chunks_inserted = []

        def mock_insert_chunk(collection, batch, page_size=1000):
            chunks_inserted.append((collection, len(batch)))

        backend._insert_chunk = mock_insert_chunk

        adapter = MockAdapterNoUpsert()
        doc = MockDocument(
            {'name': u'ES0031408433164001SV0F', 'ai': 100.0,
             'utc_timestamp': '2024-01-01 00:00:00', 'validated': True,
             'datetime': datetime.datetime(2024, 1, 1, 1, 0, 0)},
            adapter
        )

        documents = [{'f1': doc}]
        backend.insert_batch(documents)

        self.assertEqual(len(chunks_inserted), 1)
        self.assertEqual(chunks_inserted[0][0], 'tg_f1')

    def test_batch_chunking(self):
        """Documents should be split into chunks of batch_size."""
        backend = TimescaleDBBackend.__new__(TimescaleDBBackend)
        backend._columns_cache = {'tg_f1': ['name', 'ai', 'utc_timestamp']}
        backend.collection_prefix = 'tg_'
        backend.collections = ['f1']
        backend.batch_size = 2  # Very small for testing

        chunks_inserted = []

        def mock_insert_chunk(collection, batch, page_size=1000):
            chunks_inserted.append((collection, len(batch)))

        backend._insert_chunk = mock_insert_chunk

        documents = []
        for i in range(5):
            adapter = MockAdapterNoUpsert()
            doc = MockDocument(
                {'name': 'CUPS{}'.format(i), 'ai': float(i),
                 'utc_timestamp': '2024-01-01 0{}:00:00'.format(i)},
                adapter
            )
            documents.append({'f1': doc})

        backend.insert_batch(documents)

        # 5 items / batch_size=2 → 3 chunks (2 + 2 + 1)
        self.assertEqual(len(chunks_inserted), 3)
        self.assertEqual(chunks_inserted[0][1], 2)
        self.assertEqual(chunks_inserted[1][1], 2)
        self.assertEqual(chunks_inserted[2][1], 1)

    def test_gisce_collection_gets_prefix(self):
        """giscedata_* collections get tg_ prefix like any other."""
        backend = TimescaleDBBackend.__new__(TimescaleDBBackend)
        backend._columns_cache = {'tg_giscedata_corbagen': ['name', 'timestamp', 'generacio']}
        backend.collection_prefix = 'tg_'
        backend.collections = ['giscedata_corbagen']
        backend.batch_size = 500

        chunks_inserted = []

        def mock_insert_chunk(collection, batch, page_size=1000):
            chunks_inserted.append((collection, len(batch)))

        backend._insert_chunk = mock_insert_chunk

        adapter = MockAdapter(
            unique_fields=['timestamp', 'name'],
            update_fields=['generacio']
        )
        doc = MockDocument(
            {'name': 'TEST', 'timestamp': '2024-01-01', 'generacio': 100.0},
            adapter
        )
        documents = [{'giscedata_corbagen': doc}]
        backend.insert_batch(documents)

        self.assertEqual(len(chunks_inserted), 1)
        self.assertEqual(chunks_inserted[0][0], 'tg_giscedata_corbagen')


# ===========================================================================
# Tests: _prepare_cch_document (unified preparation)
# ===========================================================================

class TestPrepareCchDocument(unittest.TestCase):

    def setUp(self):
        self.backend = TimescaleDBBackend.__new__(TimescaleDBBackend)
        self.backend._columns_cache = {}

    def test_utc_timestamp_som_style(self):
        """Should compute utc_timestamp from datetime+name when column exists."""
        self.backend._columns_cache = {
            'tg_f1': ['name', 'datetime', 'utc_timestamp', 'ai']
        }

        adapter = MockAdapterNoUpsert()
        doc = MockDocument(
            {'name': u'ES0031408433164001SV0F',
             'datetime': datetime.datetime(2024, 1, 15, 10, 0, 0),
             'ai': 100.0},
            adapter
        )
        doc.collection = 'tg_f1'

        result = self.backend._prepare_cch_document(doc)

        self.assertIn('utc_timestamp', result)
        self.assertEqual(result['utc_timestamp'], '2024-01-15 09:00:00')

    def test_utc_timestamp_gisce_style(self):
        """Should compute timestamp from local_timestamp+season when column exists."""
        self.backend._columns_cache = {
            'giscedata_corbagen': ['name', 'timestamp', 'local_timestamp', 'season', 'generacio']
        }

        adapter = MockAdapter(unique_fields=['timestamp', 'name'], update_fields=['generacio'])
        doc = MockDocument(
            {'name': u'TEST', 'local_timestamp': datetime.datetime(2024, 1, 15, 10, 0, 0),
             'season': 0, 'generacio': 100.0},
            adapter
        )
        doc.collection = 'giscedata_corbagen'

        result = self.backend._prepare_cch_document(doc)

        self.assertIn('timestamp', result)
        self.assertEqual(result['timestamp'], '2024-01-15 09:00:00')

    def test_canary_utc_timestamp(self):
        """Canary Islands CUPS should use Atlantic/Canary timezone."""
        self.backend._columns_cache = {
            'tg_f1': ['name', 'datetime', 'utc_timestamp']
        }

        adapter = MockAdapterNoUpsert()
        doc = MockDocument(
            {'name': u'ES00316XXXXXXXXXXXXX',
             'datetime': datetime.datetime(2024, 1, 15, 10, 0, 0)},
            adapter
        )
        doc.collection = 'tg_f1'

        result = self.backend._prepare_cch_document(doc)
        # Canary winter is UTC+0, so 10:00 local = 10:00 UTC
        self.assertEqual(result['utc_timestamp'], '2024-01-15 10:00:00')

    def test_validated_bool_to_int(self):
        """Boolean validated should be converted to int."""
        self.backend._columns_cache = {'tg_f1': ['name', 'validated']}

        adapter = MockAdapterNoUpsert()
        doc = MockDocument({'name': u'TEST', 'validated': True}, adapter)
        doc.collection = 'tg_f1'

        result = self.backend._prepare_cch_document(doc)
        self.assertEqual(result['validated'], 1)

    def test_validated_false_to_zero(self):
        """False validated should become 0."""
        self.backend._columns_cache = {'tg_f1': ['name', 'validated']}

        adapter = MockAdapterNoUpsert()
        doc = MockDocument({'name': u'TEST', 'validated': False}, adapter)
        doc.collection = 'tg_f1'

        result = self.backend._prepare_cch_document(doc)
        self.assertEqual(result['validated'], 0)

    def test_datetime_converted_to_string(self):
        """datetime objects should be converted to string."""
        self.backend._columns_cache = {'tg_f1': ['name', 'datetime']}

        adapter = MockAdapterNoUpsert()
        doc = MockDocument(
            {'name': u'TEST', 'datetime': datetime.datetime(2024, 1, 15, 10, 30, 0)},
            adapter
        )
        doc.collection = 'tg_f1'

        result = self.backend._prepare_cch_document(doc)
        self.assertEqual(result['datetime'], '2024-01-15 10:30:00')

    def test_odoo_audit_fields(self):
        """Odoo audit fields should be set when columns exist."""
        self.backend._columns_cache = {
            'tg_f1': ['name', 'create_date', 'create_uid', 'write_date', 'write_uid']
        }

        adapter = MockAdapterNoUpsert()
        doc = MockDocument({'name': u'TEST'}, adapter)
        doc.collection = 'tg_f1'

        result = self.backend._prepare_cch_document(doc)
        self.assertIn('create_date', result)
        self.assertIn('create_uid', result)
        self.assertEqual(result['create_uid'], 1)
        self.assertIn('write_date', result)
        self.assertIn('write_uid', result)
        self.assertEqual(result['write_uid'], 1)

    def test_created_updated_at_fields(self):
        """created_at and updated_at should be set when columns exist."""
        self.backend._columns_cache = {
            'tg_f1': ['name', 'created_at', 'updated_at']
        }

        adapter = MockAdapterNoUpsert()
        doc = MockDocument({'name': u'TEST'}, adapter)
        doc.collection = 'tg_f1'

        result = self.backend._prepare_cch_document(doc)
        self.assertIn('created_at', result)
        self.assertIn('updated_at', result)

    def test_unicode_name_encoded(self):
        """Unicode name should be encoded to bytes."""
        self.backend._columns_cache = {'tg_f1': ['name']}

        adapter = MockAdapterNoUpsert()
        doc = MockDocument({'name': u'ES0031408433164001SV0F'}, adapter)
        doc.collection = 'tg_f1'

        result = self.backend._prepare_cch_document(doc)
        self.assertIsInstance(result['name'], bytes)


# ===========================================================================
# Tests: BaseBackend.insert_batch (Som error handling)
# ===========================================================================

class TestBaseBackendInsertBatch(unittest.TestCase):

    def test_insert_batch_collects_errors(self):
        """insert_batch should collect errors without stopping."""
        class FailingBackend(BaseBackend):
            call_count = 0
            def insert(self, document):
                self.call_count += 1
                if self.call_count == 2:
                    raise ValueError("Test error")

        backend = FailingBackend.__new__(FailingBackend)
        backend.call_count = 0
        errors = backend.insert_batch([{'a': 1}, {'b': 2}, {'c': 3}])

        self.assertEqual(len(errors), 1)
        self.assertIn('ValueError', errors[0])
        self.assertEqual(backend.call_count, 3)

    def test_insert_batch_no_errors(self):
        """insert_batch with no errors returns empty list."""
        class OkBackend(BaseBackend):
            def insert(self, document):
                pass

        backend = OkBackend.__new__(OkBackend)
        errors = backend.insert_batch([{'a': 1}, {'b': 2}])
        self.assertEqual(errors, [])


# ===========================================================================
# Tests: urlparse compatibility (Python 2/3)
# ===========================================================================

class TestUrlparse(unittest.TestCase):

    def test_mongodb_url(self):
        """MongoDB URL should be parsed correctly."""
        config = urlparse('mongodb://user:pass@localhost:27017/mydb')
        self.assertEqual(config['backend'], 'mongodb')
        self.assertEqual(config['username'], 'user')
        self.assertEqual(config['password'], 'pass')
        self.assertEqual(config['hostname'], 'localhost')
        self.assertEqual(config['port'], 27017)
        self.assertEqual(config['db'], 'mydb')

    def test_timescale_url(self):
        """TimescaleDB URL should be parsed correctly."""
        config = urlparse('timescale://admin:secret@db.example.com:5432/cchdb')
        self.assertEqual(config['backend'], 'timescale')
        self.assertEqual(config['username'], 'admin')
        self.assertEqual(config['password'], 'secret')
        self.assertEqual(config['hostname'], 'db.example.com')
        self.assertEqual(config['port'], 5432)
        self.assertEqual(config['db'], 'cchdb')

    def test_url_without_port(self):
        """URL without port should not fail."""
        config = urlparse('mongodb://user:pass@localhost/mydb')
        self.assertEqual(config['hostname'], 'localhost')
        self.assertEqual(config['db'], 'mydb')


# ===========================================================================
# Tests: Collection and backend configuration
# ===========================================================================

class TestTimescaleDBConfig(unittest.TestCase):

    def test_collections_include_som_and_gisce(self):
        """Collections should include both Som and GISCE types."""
        backend = TimescaleDBBackend.__new__(TimescaleDBBackend)
        # Som collections
        self.assertIn('f1', backend.collections)
        self.assertIn('p1', backend.collections)
        self.assertIn('cchfact', backend.collections)
        self.assertIn('cchval', backend.collections)
        # GISCE collections
        self.assertIn('giscedata_corbagen', backend.collections)
        self.assertIn('giscedata_epfpf', backend.collections)

    def test_collection_prefix(self):
        """Collection prefix should be 'tg_'."""
        backend = TimescaleDBBackend.__new__(TimescaleDBBackend)
        self.assertEqual(backend.collection_prefix, 'tg_')

    def test_batch_size(self):
        """Default batch size should be 500 (Som setting)."""
        backend = TimescaleDBBackend.__new__(TimescaleDBBackend)
        self.assertEqual(backend.batch_size, 500)

    def test_default_unique_fields_p1(self):
        """tg_p1 should have name+utc_timestamp+type as default unique fields."""
        self.assertEqual(
            _DEFAULT_UNIQUE_FIELDS['tg_p1'],
            ['name', 'utc_timestamp', 'type']
        )

    def test_default_unique_fallback(self):
        """Default fallback should be name+utc_timestamp."""
        self.assertEqual(_DEFAULT_UNIQUE_FALLBACK, ['name', 'utc_timestamp'])


# ===========================================================================
# Tests: insert_cch (GISCE interface preserved)
# ===========================================================================

class TestInsertCch(unittest.TestCase):

    def test_insert_cch_uses_adapter_fields(self):
        """insert_cch should use adapter unique_fields when available."""
        backend = TimescaleDBBackend.__new__(TimescaleDBBackend)
        backend._columns_cache = {
            'giscedata_epfpf': ['name', 'timestamp', 'ai', 'ae']
        }

        captured = {}

        import cchloader.backends.timescaledb as tsmod
        original_ev = tsmod.execute_values

        def mock_execute_values(cr, sql, rows, page_size=1):
            captured['sql'] = sql
            captured['rows'] = rows

        class MockCr:
            pass
        class MockDb:
            def commit(self):
                pass

        backend.cr = MockCr()
        backend.db = MockDb()

        tsmod.execute_values = mock_execute_values
        try:
            adapter = MockAdapter(
                unique_fields=['timestamp', 'name'],
                update_fields=['ai', 'ae']
            )
            doc = MockDocument(
                {'name': u'TEST', 'timestamp': '2024-01-01', 'ai': 100.0, 'ae': 50.0},
                adapter
            )
            doc.collection = 'giscedata_epfpf'

            backend.insert_cch(doc)
        finally:
            tsmod.execute_values = original_ev

        self.assertIn('ON CONFLICT (timestamp, name)', captured['sql'])
        self.assertIn('COALESCE', captured['sql'])

    def test_insert_cch_fallback_without_adapter_fields(self):
        """insert_cch should fallback to defaults for adapters without upsert fields."""
        backend = TimescaleDBBackend.__new__(TimescaleDBBackend)
        backend._columns_cache = {
            'tg_f1': ['name', 'utc_timestamp', 'ai']
        }

        captured = {}

        import cchloader.backends.timescaledb as tsmod
        original_ev = tsmod.execute_values

        def mock_execute_values(cr, sql, rows, page_size=1):
            captured['sql'] = sql
            captured['rows'] = rows

        class MockCr:
            pass
        class MockDb:
            def commit(self):
                pass

        backend.cr = MockCr()
        backend.db = MockDb()

        tsmod.execute_values = mock_execute_values
        try:
            adapter = MockAdapterNoUpsert()
            doc = MockDocument(
                {'name': u'TEST', 'utc_timestamp': '2024-01-01 00:00:00', 'ai': 100.0},
                adapter
            )
            doc.collection = 'tg_f1'

            backend.insert_cch(doc)
        finally:
            tsmod.execute_values = original_ev

        self.assertIn('ON CONFLICT (name, utc_timestamp)', captured['sql'])


if __name__ == '__main__':
    unittest.main()
