# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
import bz2
from io import BytesIO

from cchloader.parsers.parser import get_parser
from cchloader import logger
from cchloader.compress import get_compressed_file


BZ2_MAGIC = b'\x42\x5a\x68'


class PrependReader(object):
    def __init__(self, prefix, fd):
        self.prefix = prefix or b''
        self.fd = fd

    def read(self, size=-1):
        if not self.prefix:
            return self.fd.read(size)
        if size is None or size < 0:
            data = self.prefix + self.fd.read()
            self.prefix = b''
            return data
        data = self.prefix[:size]
        self.prefix = self.prefix[size:]
        if len(data) < size:
            data += self.fd.read(size - len(data))
        return data

    def readline(self):
        if not self.prefix:
            return self.fd.readline()
        newline_pos = self.prefix.find(b'\n')
        if newline_pos != -1:
            line = self.prefix[:newline_pos + 1]
            self.prefix = self.prefix[newline_pos + 1:]
            return line
        line = self.prefix + self.fd.readline()
        self.prefix = b''
        return line

    def close(self):
        self.fd.close()


class PackedCchFile(object):
    """Packed CCH file.

    Process of content of zip file is processed with iterators to keep the
    minimal memory footprint.

    Example::

        with PackedCchFile('/tmp/cch.zip') as packed:
            for cchfile in packed:
                for line in cchfile:
                    print line

    """
    def __init__(self, path, strict=False):
        """
        :param path: Packed CCH file path
        """
        self.path = path
        self.parser = get_parser(self.path)(strict=strict)

        self.filename = path
        self.file = get_compressed_file(path)

    def __iter__(self):
        return self

    def next(self):
        for filename in self.file.files:
            cch_fd = self.file.get(filename)
            sf = CchFile(filename, fd=cch_fd, parser=self.parser)
            return sf
        raise StopIteration()

    def __next__(self):
        return self.next()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the file descriptor.
        """
        self.file.close()


class CchFile(object):
    """CCH file.

    Process of content of file is processed with iterators to keep the
    minimal memory footprint.

    Example::

        with CCHFile('/tmp/cch.txt') as cch_file:
            for line in cch_file:
                print line

    :param path: Path of CCH file
    :param fd: File descriptor (use this if you have already opened the file)
    :param parser: Force to use a parser
    :param strict: Strict schema validation
    """
    def __init__(self, path, fd=None, parser=None, strict=False):
        self.path = path
        self.resume_line_number = 0
        self.errors = set()
        self._closed = False
        if parser is None:
            self.parser = get_parser(self.path)(strict=strict)
        else:
            self.parser = parser
        if fd is None:
            self.fd = open(path, 'rb')
        else:
            try:
                file_start = fd.read(5)
                if file_start.startswith(BZ2_MAGIC):
                    # bz compressed files in a zip file
                    data = bz2.decompress(file_start + fd.read())
                    bzfd = BytesIO(data)
                    self.fd = bzfd
                else:
                    # ZipExtFile has no seek method, so put back only the
                    # bytes read to detect nested compression.
                    self.fd = PrependReader(file_start, fd)
            except Exception as e:
                self.fd = fd

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    @staticmethod
    def _bytes_to_unicode(data):
        return {x: y.decode() if isinstance(y, bytes) else y for x, y in data.items()}

    def next(self):
        while True:
            line = self.fd.readline()
            if not line:
                self.close()
                break
            try:
                data, errors = self.parser.parse_line(line)
                data = self._bytes_to_unicode(data)
                if errors:
                    for error in errors.items():
                        self.errors.add(str(error))
                return data
            except Exception as exc:
                logger.critical(
                    # "Error in %s L:%s: %s %s", self.path, self.stats.line_number,
                    "Error in %s %s %s", self.path, str(exc), line, exc_info=True
                )
                if self.parser.adapter.strict:
                    raise
        raise StopIteration()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self._closed:
            return
        self.fd.close()
        self._closed = True
