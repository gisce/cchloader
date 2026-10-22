# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest

from cchloader.parsers.p1 import P1
from cchloader.parsers.p1d import P1D
from cchloader.parsers.p2 import P2
from cchloader.parsers.p2d import P2D


class TestDecimalQualities(unittest.TestCase):

    decimal_quality_line = (
        b'ES00000000000000000000;11;2026/01/31 12:00:00;0;14.0;0.0;'
        b'0.0;0.0;0.0;128.0;0.0;0.0;0.0;0.0;0;0.0;0.0;0;'
        b'0.0;0;1;1;\n'
    )

    def test_p1d_accepts_integral_decimal_qualities(self):
        parsed, errors = P1D().parse_line(self.decimal_quality_line)
        data = parsed['p1'].data

        self.assertEqual(errors, {})
        self.assertEqual(data['aoquality'], 0)
        self.assertEqual(data['r1quality'], 128)

    def test_all_p_parsers_accept_integral_decimal_qualities(self):
        for parser_class in (P1, P1D, P2, P2D):
            parsed, errors = parser_class().parse_line(
                self.decimal_quality_line
            )
            data = parsed['p1'].data

            self.assertEqual(errors, {})
            self.assertEqual(data['aiquality'], 0)
            self.assertEqual(data['r1quality'], 128)

    def test_fractional_quality_is_rejected_without_truncation(self):
        line = self.decimal_quality_line.replace(b';128.0;', b';1.5;', 1)

        parsed, errors = P1D().parse_line(line)

        self.assertIn('r1quality', errors)
        self.assertNotIn('r1quality', parsed['p1'])

    def test_out_of_range_decimal_quality_is_rejected(self):
        line = self.decimal_quality_line.replace(b';128.0;', b';255.0;', 1)

        parsed, errors = P1D().parse_line(line)

        self.assertIn('r1quality', errors)
        self.assertNotIn('r1quality', parsed['p1'])


if __name__ == '__main__':
    unittest.main()
