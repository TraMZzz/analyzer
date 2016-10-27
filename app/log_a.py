# -*- coding: utf-8 -*-

import re
from datetime import datetime, timedelta
import pandas as pd


class LogAnalyzer(object):
    # TODO: need refactoring

    def __init__(self):
        self.data = {}
        self.max_micro = 100 * 1000
        self.if_print = True

    def analyze(self, logfile, if_print=True):
        self.if_print = if_print
        msg_re = re.compile('messageId=(MSG_.{13})', re.I | re.S)
        date_re = re.compile('^\d{2}:\d{2}:\d{2},\d{3}')

        # open file
        log = open(logfile, 'r')
        lines = log.readlines()
        log.close()

        # get date and id from each line
        for line in lines:
            match = msg_re.search(line)
            match_date = date_re.search(line)
            m_date = match_date.group(0) if match_date else ''
            if match:
                m_id = match.group(1)
                m_id_data = self.data.get(m_id)
                if m_id_data:
                    m_id_data.append(m_date)
                    self.data[m_id] = m_id_data
                else:
                    self.data[m_id] = [m_date]
        if self.if_print:
            self.grouping()
        else:
            return self.grouping()

    def grouping(self):
        # check transaction is successful or not
        successful = {}
        unsuccessful = {}
        for key in self.data.keys():
            values = self.data[key]
            if len(values) == 4:
                delta = self.str_date(values[-1]) - self.str_date(values[0])
                if delta > timedelta(microseconds=self.max_micro):
                    unsuccessful[key] = values[-1]
                else:
                    successful[key] = values[-1]
            else:
                unsuccessful[key] = values[-1]
        if self.if_print:
            self.show_data(successful, unsuccessful)
        else:
            return self.show_data(successful, unsuccessful)

    def pandas_group(self, group, T):
        # build datetime frame
        df = pd.DataFrame(group.items(), columns=['count', 'date'])
        df = df.set_index(pd.DatetimeIndex(pd.to_datetime(df['date'])))
        df = df.drop(['date'], axis=1)
        return df.resample(T, how='count')

    def show_data(self, successful, unsuccessful):
        data = {}
        if successful:
            s_per_min = self.pandas_group(successful, '1T')
            s_per_hour = self.pandas_group(successful, '60T')
            if self.if_print:
                print("Successful:")
                print("per 1 min:")
                print(s_per_min)
                print("per 1 hour:")
                print(s_per_hour)
            else:
                data['s_per_min'] = s_per_min.to_dict()
                data['s_per_hour'] = s_per_hour.to_dict()
        if unsuccessful:
            un_per_min = self.pandas_group(unsuccessful, '1T')
            un_per_hour = self.pandas_group(unsuccessful, '60T')
            if self.if_print:
                print("Unsuccessful:")
                print("per 1 min:")
                print(un_per_min)
                print("per 1 hour:")
                print(un_per_hour)
            else:
                data['un_per_min'] = un_per_min.to_dict()
                data['un_per_hour'] = un_per_hour.to_dict()
        if not self.if_print:
            return data

    def str_date(self, date):
        return datetime.strptime(date, '%H:%M:%S,%f')
