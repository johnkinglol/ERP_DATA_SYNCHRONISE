#!/usr/bin/env python
# coding=utf-8
import os
import logging
import logging.handlers
import ConfigParser
import MySQLdb
import itertools
import datetime
import calendar

STATUS_OK = 0
STATUS_FAILED = -1


class BasicResult(object):
    def __init__(self, result_code=STATUS_OK, result_info='ok'):
        self.result_code = result_code
        self.result_info = result_info

    def __str__(self):
        return "result code : %d, result info : %s " % (self.result_code, self.result_info)

    def is_failed(self):
        return self.result_code == STATUS_FAILED

    # def reset(self, result_code=0, result_info='ok'):
    #    self.result_code = result_code
    #    self.result_info = result_info


class ProcessError(UserWarning):
    def __init__(self, message):
        self.error_info = message

    def __str__(self):
        return self.error_info


class DbClient(object):
    def __init__(self):
        self._conf_parse = ConfigParser.ConfigParser()
        self._host = ''
        self._port = 3306
        self._user = ''
        self._pass_word = ''
        self._connect_timeout = 5
        self._connection = ''
        self._cursor = ''
        self._is_init = False
        self.db_result = BasicResult()

    def init_connect(self, conf_file, db_title_name):
        if not os.path.exists(conf_file):
            return BasicResult(STATUS_FAILED, 'configure file is not exist')
        self._conf_parse.read(conf_file)
        self._host = self._conf_parse.get(db_title_name, 'ip')
        self._port = self._conf_parse.getint(db_title_name, 'port')
        self._user = self._conf_parse.get(db_title_name, 'user')
        self._pass_word = self._conf_parse.get(db_title_name, 'passwd')
        self._connect_timeout = self._conf_parse.getint(db_title_name, 'timeout')
        try:
            self._connection = MySQLdb.connect(
                host=self._host, port=self._port, user=self._user,
                passwd=self._pass_word, connect_timeout=self._connect_timeout)
            self._cursor = self._connection.cursor(MySQLdb.cursors.DictCursor)
            self._is_init = True
            return BasicResult()
        except MySQLdb.Error, error:
            self._is_init = False
            print("mysql error %d: %s" % (error.args[0], error.args[1]))
            return BasicResult(STATUS_FAILED, error.args[1])

    def execute(self, sql, need_fetch=False, auto_commit=True):
        try:
            self.db_result = BasicResult()
            self._cursor.execute(sql)
            if need_fetch:
                self.db_result.result_info = self._cursor.fetchall()
            if auto_commit:
                self._connection.commit()
            return self.db_result
        except MySQLdb.Error, error:
            if not need_fetch:
                self._connection.rollback()
            print("mysql error %s" % error)
            return BasicResult(STATUS_FAILED, error.args[1])

    def query(self, sql):
        try:
            self.db_result = BasicResult()
            self._cursor.execute(sql)
            self.db_result.result_info = self._cursor.fetchall()
            return self.db_result
        except MySQLdb.Error, error:
            print("mysql error %s" % error)
            return BasicResult(STATUS_FAILED, error.args[1])

    def execute_select(self, sql):
        try:
            self.db_result = BasicResult()
            self._cursor.execute(sql)
            self.db_result.result_info = self._cursor.fetchall()
            return self.db_result
        except MySQLdb.Error, error:
            print("mysql error %s" % error)
            return BasicResult(STATUS_FAILED, error.args[1])

    def execute_update(self, sql):
        try:
            self.db_result = BasicResult()
            self._cursor.execute(sql)
            self._connection.commit()
            return self.db_result
        except MySQLdb.Error, error:
            self._connection.rollback()
            print("mysql error %s" % error)
            return BasicResult(STATUS_FAILED, error.args[1])

    def update(self, sql):
        try:
            self.db_result = BasicResult()
            self._cursor.execute(sql)
            self._connection.commit()
            return self.db_result
        except MySQLdb.Error, error:
            self._connection.rollback()
            print("mysql error %s %s" % (error, sql))
            return BasicResult(STATUS_FAILED, error.args[1])

    def execute_no_commit(self, sql):
        try:
            self.db_result = BasicResult()
            self._cursor.execute(sql)
            return self.db_result
        except MySQLdb.Error, error:
            print("mysql error %s" % error)
            return BasicResult(STATUS_FAILED, error.args[1])

    def commit(self):
        self._connection.commit()

    def close(self):
        if self._is_init:
            self._cursor.close()
            self._connection.close()
            self._is_init = False

    def __del__(self):
        try:
            self.close()
        except MySQLdb.Error, error:
            print BasicResult(STATUS_FAILED, "db_client __del__ failed %s" % error)


class FileLogger(object):
    def __init__(self):
        self._conf_parse = ConfigParser.ConfigParser()
        self.logger = None

    def init_logger(self, conf_file, date, log_module_name='file'):
        if not os.path.exists(conf_file):
            return BasicResult(STATUS_FAILED, 'log configure file is not exist')
        self._conf_parse.read(conf_file)
        log_name = self._conf_parse.get('log', 'log_name')
        log_name = "{}_{}.log".format(log_name, date)
        log_level = self._conf_parse.get('log', 'log_level')
        if log_level == 'debug':
            log_level = logging.DEBUG
        elif log_level == 'info':
            log_level = logging.INFO
        elif log_level == 'error':
            log_level = logging.ERROR
        elif log_level == 'fatal':
            log_level = logging.FATAL
        else:
            log_level = logging.DEBUG

        self.logger = logging.getLogger(log_module_name)
        self.logger.setLevel(log_level)

        file_handler = logging.FileHandler(log_name, 'a')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(logging.Formatter('[%(asctime)s %(module)s] %(levelname)s %(message)s'))
        self.logger.addHandler(file_handler)

        stream_handle = logging.StreamHandler()
        stream_handle.setLevel(logging.ERROR)
        self.logger.addHandler(stream_handle)

        # delete old file log
        if len(date) == 10:
            date_time_now = datetime.datetime.strptime(date, "%Y-%m-%d")
            date_time_old = date_time_now.date() - datetime.timedelta(90)
            log_name = self._conf_parse.get('log', 'log_name')
            old_log_name = log_name = "{}_{}.log".format(log_name, date_time_old.strftime("%Y-%m-%d"))
            # print("old log file name is {}".format(old_log_name))
            if os.path.exists(old_log_name):
                os.remove(old_log_name)
        return BasicResult(STATUS_OK, self.logger)

    def debug(self, msg, *args, **kwargs):
        if self.logger:
            self.logger.debug(msg, *args, **kwargs)
        else:
            logging.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        if self.logger:
            self.logger.info(msg, *args, **kwargs)
        else:
            logging.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        if self.logger:
            self.logger.warning(msg, *args, **kwargs)
        else:
            logging.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        if self.logger:
            self.logger.error(msg, *args, **kwargs)
        else:
            logging.error(msg, *args, **kwargs)

    def fatal(self, msg, *args, **kwargs):
        if self.logger:
            self.logger.fatal(msg, *args, **kwargs)
        else:
            logging.fatal(msg, *args, **kwargs)


class MonitorMail(object):
    def __init__(self):
        self._conf_parse = ConfigParser.ConfigParser()
        self.mail_to = "xiangtaoli"
        self.mail_title = ''
        self.mail_file = ''
        self.mail_handle = None

    def init_mail(self, conf_file, date=''):
        if not os.path.exists(conf_file):
            return BasicResult(STATUS_FAILED, 'configure file is not exist')
        self._conf_parse.read(conf_file)
        try:
            mail_file = self._conf_parse.get('monitor', 'mail_file')
            mail_to = self._conf_parse.get('monitor', 'mail_to')
            mail_title = self._conf_parse.get('monitor', 'mail_title')
        except ConfigParser.NoSectionError, error:
            return BasicResult(STATUS_FAILED, "get mail conf failed %s" % error)
        except ConfigParser.NoOptionError, error:
            return BasicResult(STATUS_FAILED, "get mail conf failed %s" % error)

        self.mail_file = "%s_%s" % (mail_file, date)
        self.mail_to = mail_to
        temp = mail_title.decode("utf-8").encode('gbk', 'ignore')
        self.mail_title = "%s_%s" % (date, temp)
        try:
            self.mail_handle = open(self.mail_file, "w")
        except IOError, error:
            self.mail_handle = None
            return BasicResult(STATUS_FAILED, "open mail file failed: %s" % error)
        return BasicResult()

    def write_line(self, line):
        self.mail_handle.write(line + os.linesep)

    def send_mail(self):
        if self.mail_handle:
            self.mail_handle.close()
        else:
            return None

        if not os.path.exists(self.mail_file):
            return None
        if not os.path.exists("/bin/sendmail2"):
            return None
        send_mail_cmd = "/bin/sendmail2 'oss-admin' '%s' '' '%s' '%s'" % \
                        (self.mail_to, self.mail_title, self.mail_file)
        os.system(send_mail_cmd)


# when using, need catch IOError
class FileParser(object):
    def __init__(self, file_name, filed_list, filed_sep):
        self.file_name = file_name
        self.filed_list = filed_list
        self.filed_sep = filed_sep
        self.file_handle = None

    def __iter__(self):
        # try:
        self.file_handle = open(self.file_name, 'r')
        # except IOError as error:
        #     print("FileParser open {0} failed: {1}".format(self.file_name, error))
        #     raise error

        return self

    def next(self):
        # try:
        line = self.file_handle.readline()
        if line == '':
            self.file_handle.close()
            raise StopIteration
        if line[-1] == os.linesep:
            line_array = line[:-1].split(self.filed_sep)
        else:
            line_array = line.split(self.filed_sep)
        line_dict = dict(itertools.izip(self.filed_list, line_array))
        # except IOError as error:
        #     print("FileParser read {0} failed: {1}".format(self.file_name, error))
        #     raise error

        return line_dict

    def close(self):
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None


def parse_title(title_line, sep=""):
    index = 0
    field_map = {}
    # cut the last '\n'
    if title_line.endswith(os.linesep):
        title_array = title_line[:-1].split(sep)
    else:
        title_array = title_line.split(sep)
    for field in title_array:
        field_map[field] = index
        index += 1

    if 0 == len(field_map):
        return None

    return field_map


def parse_title_file(title_file):
    if not os.path.exists(title_file):
        return None

    file_handle = open(title_file, 'r')
    fields_map = {}
    index = 0
    for line in file_handle:
        line = line.strip()
        fields_map[line] = index
        index += 1
    file_handle.close()

    if 0 == len(fields_map):
        return None

    return fields_map


def parse_line(line, fields_map, sep=""):
    if len(line) == 0:
        return None
    if line[-1] == os.linesep:
        line_array = line[:-1].split(sep)
    else:
        line_array = line.split(sep)

    array_len = len(line_array)
    if len(line_array) == 0:
        return None
    if len(fields_map) == 0:
        return None

    line_dict = {}
    for k, v in fields_map.items():
        if v >= array_len:
            return None
        line_dict[k] = line_array[v]

    return line_dict


def send_email(mail_to, title, mail_file):
    if not os.path.exists(mail_file):
        return None
    if not os.path.exists("/bin/sendmail2"):
        return None
    send_mail_cmd = "/bin/sendmail2 'oss-admin' '%s' '' '%s' '%s'" % \
                    (mail_to, title, mail_file)
    os.system(send_mail_cmd)


# date or month string: 2015-03-28 or 2015-03
def get_last_month(date_string):
    temp_array = date_string.split("-")
    year = int(temp_array[0])
    month = int(temp_array[1])
    # day = int(temp_array[2])

    # now_date = datetime.date(year, month, day)
    # first_day = datetime.date(now_date.year, now_date.month, 1)
    first_day = datetime.date(year, month, 1)
    last_month_day = first_day - datetime.timedelta(days=1)
    return last_month_day.strftime("%Y-%m")


def get_next_month(date_string):
    temp_array = date_string.split("-")
    year = int(temp_array[0])
    month = int(temp_array[1])
    if month == 12:
        month = 1
        year += 1
    else:
        month += month
    return "%4d-%02d" %(year, month)


# month string : 2015-03
def get_month_days(month_string):
    month_list = month_string.split('-')
    year = int(month_list[0])
    month = int(month_list[1])

    return calendar.monthrange(year, month)[1]
