# -*- coding: utf-8 -*-
from os import path
import shutil
import tempfile
import unittest

from log_a import LogAnalyzer


class TestAnalyzer(unittest.TestCase):
    # TODO: need refactoring

    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        # Remove the directory after the test
        shutil.rmtree(self.test_dir)

    def test_unsuccessful_len(self):
        file = path.join(self.test_dir, 'test.txt')
        f = open(file, 'w')
        f.write('06:29:57,307 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549212')
        f.close()
        analyzer = LogAnalyzer().analyze(file, False)
        # check if unsuccessful
        self.assertEqual(analyzer['un_per_min']['count'].values()[0], 1)
        self.assertEqual(analyzer['un_per_hour']['count'].values()[0], 1)

        # check if successful
        self.assertEqual(analyzer.get('s_per_hour'), None)
        self.assertEqual(analyzer.get('s_per_hour'), None)

    def test_unsuccessful_time(self):
        file = path.join(self.test_dir, 'test.txt')
        f = open(file, 'w')
        line = '06:29:57,307 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549212'
        line2 = '06:29:57,707 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549212'
        f.write('{}\n{}\n{}\n{}\n'.format(line, line, line, line2))
        f.close()
        analyzer = LogAnalyzer().analyze(file, False)
        # check if unsuccessful
        self.assertEqual(analyzer['un_per_min']['count'].values()[0], 1)
        self.assertEqual(analyzer['un_per_hour']['count'].values()[0], 1)

        # check if successful
        self.assertEqual(analyzer.get('s_per_min'), None)
        self.assertEqual(analyzer.get('s_per_hour'), None)

    def test_successful(self):
        file = path.join(self.test_dir, 'test.txt')
        f = open(file, 'w')
        line = '06:29:57,307 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549212'
        f.write('{}\n{}\n{}\n{}\n'.format(line, line, line, line))
        f.close()
        analyzer = LogAnalyzer().analyze(file, False)
        # check if successful
        self.assertEqual(analyzer['s_per_min']['count'].values()[0], 1)
        self.assertEqual(analyzer['s_per_hour']['count'].values()[0], 1)

        # check if unsuccessful
        self.assertEqual(analyzer.get('un_per_min'), None)
        self.assertEqual(analyzer.get('un_per_hour'), None)

    def test_successful_unsuccessful(self):
        file = path.join(self.test_dir, 'test.txt')
        f = open(file, 'w')
        line = '06:29:57,307 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549212'
        line2 = '06:29:57,707 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549213'
        f.write('{}\n{}\n{}\n{}\n{}\n{}\n{}\n'.format(
            line, line, line, line2, line2, line2, line2))
        f.close()
        analyzer = LogAnalyzer().analyze(file, False)
        # check if unsuccessful
        self.assertEqual(analyzer['un_per_min']['count'].values()[0], 1)
        self.assertEqual(analyzer['un_per_hour']['count'].values()[0], 1)

        # check if successful
        self.assertEqual(analyzer.get('s_per_min')['count'].values()[0], 1)
        self.assertEqual(analyzer.get('s_per_hour')['count'].values()[0], 1)

    def test_successful_per_time_min(self):
        file = path.join(self.test_dir, 'test.txt')
        f = open(file, 'w')
        line = '06:29:57,307 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549212'
        line2 = '06:29:57,507 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549213'
        f.write('{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n'.format(
            line, line, line, line, line2, line2, line2, line2))
        f.close()
        analyzer = LogAnalyzer().analyze(file, False)

        # check if unsuccessful
        self.assertEqual(analyzer.get('un_per_min'), None)
        self.assertEqual(analyzer.get('un_per_hour'), None)

        # check if successful
        self.assertEqual(len(analyzer.get('s_per_min')['count']), 1)
        self.assertEqual(len(analyzer.get('s_per_hour')['count']), 1)
        self.assertEqual(analyzer.get('s_per_min')['count'].values()[0], 2)
        self.assertEqual(analyzer.get('s_per_hour')['count'].values()[0], 2)

    def test_successful_multy_per_time_min(self):
        file = path.join(self.test_dir, 'test.txt')
        f = open(file, 'w')
        line = '06:29:00,307 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549212'
        line2 = '06:29:00,507 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549213'
        line3 = '06:30:00,507 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549215'
        f.write('{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n'.format(
            line, line, line, line, line2, line2, line2, line2, line3, line3, line3, line3))
        f.close()
        analyzer = LogAnalyzer().analyze(file, False)

        # check if unsuccessful
        self.assertEqual(analyzer.get('un_per_min'), None)
        self.assertEqual(analyzer.get('un_per_hour'), None)

        # check if successful
        self.assertEqual(len(analyzer.get('s_per_min')['count']), 2)
        self.assertEqual(len(analyzer.get('s_per_hour')['count']), 1)
        self.assertEqual(analyzer.get('s_per_min')['count'].values()[0], 2)
        self.assertEqual(analyzer.get('s_per_min')['count'].values()[1], 1)
        self.assertEqual(analyzer.get('s_per_hour')['count'].values()[0], 3)

    def test_unsuccessful_per_time_min(self):
        file = path.join(self.test_dir, 'test.txt')
        f = open(file, 'w')
        line = '06:29:57,307 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549212'
        line2 = '06:29:57,507 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549213'
        f.write('{}\n{}\n{}\n{}\n{}\n{}\n'.format(line, line, line, line2, line2, line2))
        f.close()
        analyzer = LogAnalyzer().analyze(file, False)

        # check if unsuccessful
        self.assertEqual(analyzer.get('s_per_min'), None)
        self.assertEqual(analyzer.get('s_per_hour'), None)

        # check if successful
        self.assertEqual(len(analyzer.get('un_per_min')['count']), 1)
        self.assertEqual(len(analyzer.get('un_per_hour')['count']), 1)
        self.assertEqual(analyzer.get('un_per_min')['count'].values()[0], 2)
        self.assertEqual(analyzer.get('un_per_hour')['count'].values()[0], 2)

    def test_unsuccessful_multy_per_time_min(self):
        file = path.join(self.test_dir, 'test.txt')
        f = open(file, 'w')
        line = '06:29:00,307 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549212'
        line2 = '06:29:00,507 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549213'
        line3 = '06:30:00,507 INFO Broker:56 - Broker got message from Queue=0, messageId=MSG_527ccb3549215'
        f.write('{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n'.format(
            line, line, line, line2, line2, line2, line3, line3, line3))
        f.close()
        analyzer = LogAnalyzer().analyze(file, False)

        # check if unsuccessful
        self.assertEqual(analyzer.get('s_per_min'), None)
        self.assertEqual(analyzer.get('s_per_hour'), None)

        # check if successful
        self.assertEqual(len(analyzer.get('un_per_min')['count']), 2)
        self.assertEqual(len(analyzer.get('un_per_hour')['count']), 1)
        self.assertEqual(analyzer.get('un_per_min')['count'].values()[0], 2)
        self.assertEqual(analyzer.get('un_per_min')['count'].values()[1], 1)
        self.assertEqual(analyzer.get('un_per_hour')['count'].values()[0], 3)

    def test_empty_file(self):
        file = path.join(self.test_dir, 'test.txt')
        f = open(file, 'w')
        f.write('')
        f.close()
        analyzer = LogAnalyzer().analyze(file, False)

        self.assertEqual(len(analyzer), 0)

if __name__ == '__main__':
    unittest.main()
