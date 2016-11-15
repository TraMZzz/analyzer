# -*- coding: utf-8 -*-
import sys

import re
from datetime import datetime, timedelta
import pandas as pd


class LogAnalyzer(object):
    # TODO: need refactoring

    def __init__(self):
        self.data = {}
        self.max_micro = 100 * 1000
        self.if_print = True
        self.msg_re = re.compile('messageId=(MSG_.{13})', re.I | re.S)
        self.date_re = re.compile('^\d{2}:\d{2}:\d{2},\d{3}')

    def read_file(self, logfile):
        # read file
        lines = []
        try:
            with open(logfile, "r") as outfile:
                lines = outfile.readlines()
        except IOError as e:
            print "I/O error({0}): {1}".format(e.errno, e.strerror)
        except:
            print "Unexpected error:", sys.exc_info()[0]
            raise
        return lines

    def analyze(self, logfile, if_print=True):
        self.if_print = if_print
        self.data = {}

        # get date and id from each line
        for line in self.read_file(logfile):
            match = self.msg_re.search(line)
            match_date = self.date_re.search(line)
            m_date = match_date.group(0) if match_date else ''
            m_id = match.group(1) if match else ''
            m_id_data = self.data.get(m_id)
            if m_id_data:
                m_id_data.append(m_date)
                self.data[m_id] = m_id_data
            else:
                self.data[m_id] = [m_date]

        successful, unsuccessful = self.grouping()
        if self.if_print:
            self.print_data(successful, unsuccessful)
        else:
            return self.get_data_dict(successful, unsuccessful)

    def grouping(self):
        # check transaction is successful or not
        successful = {}
        unsuccessful = {}
        for key in self.data.keys():
            values = self.data[key]
            delta = self.str_date(values[-1]) - self.str_date(values[0])
            if len(values) == 4 and delta <= timedelta(microseconds=self.max_micro):
                successful[key] = values[-1]
            else:
                unsuccessful[key] = values[-1]
        return successful, unsuccessful

    def pandas_group(self, group, T, to_dict=False):
        # build datetime frame
        if not group:
            return None
        df = pd.DataFrame(group.items(), columns=['count', 'date'])
        df = df.set_index(pd.DatetimeIndex(pd.to_datetime(df['date'])))
        df = df.drop(['date'], axis=1)
        frame = df.resample(T, how='count')
        if to_dict:
            return frame.to_dict()
        return frame

    def get_data_dict(self, successful, unsuccessful):
        data = {}
        data['s_per_min'] = self.pandas_group(successful, '1T', True)
        data['s_per_hour'] = self.pandas_group(successful, '60T', True)
        data['un_per_min'] = self.pandas_group(unsuccessful, '1T', True)
        data['un_per_hour'] = self.pandas_group(unsuccessful, '60T', True)
        return data

    def print_data(self, successful, unsuccessful):
        print("Successful:")
        print("per 1 min:")
        print(self.pandas_group(successful, '1T'))
        print("per 1 hour:")
        print(self.pandas_group(successful, '60T'))
        print("Unsuccessful:")
        print("per 1 min:")
        print(self.pandas_group(unsuccessful, '1T'))
        print("per 1 hour:")
        print(self.pandas_group(unsuccessful, '60T'))

    def str_date(self, date):
        return datetime.strptime(date, '%H:%M:%S,%f')
