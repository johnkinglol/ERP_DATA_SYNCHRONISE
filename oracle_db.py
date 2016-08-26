#!/usr/bin/env python
# coding=utf-8
import os
import ConfigParser
import cx_Oracle
from cx_Oracle import Cursor


STATUS_OK = 0
STATUS_FAILED = -1
FILE_TYPE_TEXT = 1
FILE_TYPE_GZIP = 2

class BasicResult(object):
    def __init__(self, result_code=STATUS_OK, result_info='ok'):
        self.result_code = result_code
        self.result_info = result_info

    def __str__(self):
        return "result code : {}, result info : {} ".format(self.result_code, self.result_info)

    def is_failed(self):
        return self.result_code == STATUS_FAILED


class DbClient(object):
    def __init__(self):
        self._conf_parse = ConfigParser.ConfigParser()
        self._host = ""
        self._port = 3306
        self._user = ""
        self._pass_word = ""
        self._connect_timeout = 5
        self._connect_character = ""
        self._connection = ""
        self._cursor = ""
        self._db_type = "mysql"
        self._is_init = False
        self.db_result = BasicResult()

    def init_connect(self, conf_file, db_title_name):
        if not os.path.exists(conf_file):
            return BasicResult(STATUS_FAILED, 'configure file is not exist')
        self._conf_parse.read(conf_file)
        self._host = self._conf_parse.get(db_title_name, 'tns')
        self._user = self._conf_parse.get(db_title_name, 'user')
        self._pass_word = self._conf_parse.get(db_title_name, 'passwd')
        try:
            self._connection = cx_Oracle.connect(self._user, self._pass_word, self._host)
            self._cursor = self._connection.cursor()
            self._is_init = True
            return BasicResult()
        except cx_Oracle.DatabaseError as error:
            self._is_init = False
            print("oracle error {}: {}".format(error.args[0], error.args[1]))
            return BasicResult(STATUS_FAILED, error.args[1])

    def makedict(self, cursor):
        cols = [d[0] for d in cursor.description]
        def createrow(*args):
            return dict(zip(cols, args))
        return createrow

    def query(self, sql):
        try:
            self.db_result = BasicResult()
            self._cursor.execute(sql)
            self.db_result.result_info = self._cursor.fetchall()
            return self.db_result
        except cx_Oracle.Error as error:
            print("oracle error {}".format(error))
            return BasicResult(STATUS_FAILED, error.args[1])

    def procedure_query(self, name):
        try:
            sth_cursor = self._cursor.var(cx_Oracle.CURSOR)
            #sth_cursor.rowfactory = self.makedict(sth_cursor)
            cursor = self._cursor.callproc(name,[sth_cursor])
            #print(cursor[0].description)
            cursor[0].rowfactory = self.makedict(cursor[0])
            self.db_result.result_info = cursor[0].fetchall()

            return self.db_result
        except cx_Oracle.Error as error:
            print("oracle error {}".format(error))
            return BasicResult(STATUS_FAILED, error.args[1])

    def update(self, sql):
        try:
            self.db_result = BasicResult()
            self._cursor.execute(sql)
            self._connection.commit()

            return self.db_result
        except cx_Oracle.Error as error:
            self._connection.rollback()
            print("oracle error {}".format(error))
            return BasicResult(STATUS_FAILED, error.args[1])

    def commit(self):
        self._connection.commit()

    def close(self):
        if self._is_init:
            self._cursor.close()
            self._connection.close()
            self._is_init = False
