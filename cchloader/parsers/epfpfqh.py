# -*- encoding: utf-8 -*-
from __future__ import absolute_import

from cchloader.parsers.parser import register
from cchloader.parsers.epfpf import EPFPF
from datetime import datetime, timedelta
from pytz import timezone


import six
if six.PY3:
    unicode = str

class EPFPFQH(EPFPF):

    patterns = [
        # Documented
        '^EPFPFQH_(\w{2})_(\w{3})_(\w{4})_(\w{2})_(\d{8})\.(\d+)',
    ]
    encoding = "iso-8859-15"
    delimiter = ';'

    def parse_line(self, line, filename = None):
        parsed, errors = super(EPFPFQH, self).parse_line(line=line, filename=filename)

        # Add type 'p4'
        result = parsed.get('giscedata_epfpf')
        result.data['type'] = 'p4'
        parsed['giscedata_epfpf'] = result
        return parsed, errors

    def get_datetime_and_season(self, data):
        year = int(data.get('year'))
        month = int(data.get('month'))
        day = int(data.get('day'))
        periodo = int(data.get('periodo'))
        dt = datetime(year=year, month=month, day=day)
        mad_tz = timezone('Europe/Madrid')
        local_datetime = mad_tz.localize(dt, is_dst=None)
        hours = timedelta(minutes=periodo*15)
        final_date = mad_tz.normalize(local_datetime + hours)
        dt = final_date.strftime('%Y-%m-%d %H:%M:%S')
        dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
        season = 1 if final_date.dst().total_seconds() == 3600 else 0
        return dt, season


register(EPFPFQH)