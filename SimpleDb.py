# !/usr/bin/env python
# -*- coding: UTF-8 -*-
####
# version:2019-11-20
# author:lvshao
####
import MySQLdb as DB
import MySQLdb.cursors as cursors
import array


class SimpleDb(object):
    
    def __init__(self, host, user, passwd, db, cursorclass=cursors.SSDictCursor, port=3306, charset='utf8',
                 autocommit=False):
        self._db_conf = {
            'user': user,
            'passwd': passwd,
            'host': host,
            'db': db,
            'port': int(port),
            'charset': charset,
            'cursorclass': cursorclass,
        }
        self.conn = None
        self.cursor = None
        self.cursor_class = cursorclass
        self._autocommit = autocommit
        self.last_execute_sql = None
        self.conn = self.get_conn()
        self.cursor = self.get_new_cursor(self.cursor_class)
        self.count = 0
        self.last_sql_tmp = ''
    
    def __enter__(self):
        self.get_conn()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
    
    def get_conn(self):
        try:
            self.conn = DB.connect(**self._db_conf)
        except Exception as e:
            raise e
        return self.conn
    
    def get_new_cursor(self, cursor_type=None):
        if not cursor_type:
            cursor_type = cursors.SSDictCursor
        try:
            self.cursor = self.conn.cursor(cursor_type)
        except Exception as e:
            raise e
        return self.cursor
    
    def __commit(self, commit):
        if commit is True:
            self.cursor = ''
            self.commit()
            self.get_new_cursor(self.cursor_class)
        elif commit is None:
            if self._autocommit:
                self.cursor = ''
                self.commit()
                self.get_new_cursor(self.cursor_class)
    
    def commit(self):
        self.conn.commit()
    
    def rollback(self):
        self.conn.rollback()
    
    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            self.conn.close()
        except Exception as e:
            raise e
    
    # 返回最后执行的sql语句
    def get_last_sql(self):
        if isinstance(self.cursor_class, cursors.DictCursor):
            if self.cursor._last_executed:
                decoded = array.array('b', self.cursor._last_executed).tostring().decode('utf-8')
                return decoded
            else:
                return ''
        else:
            return self.last_sql_tmp
    
    # 只接受 dict类型数据或者一组dict类型数据
    def __is_data_many(self, data):
        if (isinstance(data, list) or isinstance(data, tuple)) and isinstance(data[0], dict):
            return True
        elif isinstance(data, dict):
            return False
        raise Exception("data type error", "data must is a dict or dict list")
    
    def __is_execute_data_many(self, data):
        if (isinstance(data, list) or isinstance(data, tuple)) and (
                isinstance(data[0], list) or isinstance(data[0], tuple)):
            return True
        else:
            return False
    
    # 支持 as 别名字段 例如"x_field as xxx"
    def format_fields(self, fields):
        field_str = ','.join(["`{0}` as `{1}`".format(*f.split(' as ')) if ' as ' in f else "`%s`" % f for f in fields])
        return field_str
    
    # 批量格式化 in 条件数据
    def format_in(self, list):
        str = ','.join(["'%s'" % each for each in list])
        return str
    
    # 批量格式化 %s
    def generate_s(self, fields):
        length = len(fields)
        s_string = ",".join(["%s" for i in range(length)])
        return s_string
    
    # 只有None 和 ''为空
    def _is_real_empty(self, value):
        if value is None:
            return True
        if value is '':
            return True
        return False
    
    # 默认值为 ''
    def _deal_default(self, default_value):
        if default_value is None:
            return ''
        else:
            return default_value
    
    # 格式化传入的数据
    def format_data(self, fields, data, defaults={}, is_many=False):
        if is_many:
            data_list = []
            for i, each in enumerate(data):
                data_list.append(
                    list([each.get(field) if not self._is_real_empty(each.get(field)) else self._deal_default(
                        defaults.get(field)) if defaults else '' for field in fields]))
            return data_list
        else:
            data = [data.get(field) if not self._is_real_empty(data.get(field)) else self._deal_default(
                defaults.get(field)) if defaults else '' for field in fields]
            return data
    
    # 获取表的字段
    def get_table_fields(self, table):
        try:
            self.cursor.execute("SHOW fields FROM %s" % table)
            fields = tuple([each['Field'] for each in self.cursor.fetchall()])
            return fields
        except Exception as e:
            raise e
    
    # 执行传入的sql语句
    def execute(self, query, args=None, commit=None, get_first=False, return_iterator=False):
        try:
            is_many = self.__is_execute_data_many(args) if args else False
            self.last_sql_tmp = query
            if not args:
                self.cursor.execute(query, args)
            else:
                if is_many:
                    self.cursor.executemany(query, args)
                else:
                    self.cursor.execute(query, args)
            
            if get_first or not return_iterator:
                result = self.__get_execute_result(commit=commit, get_first=get_first)
            else:
                result = self.__get_execute_result_iterator(commit=commit)
            
            return result
        except Exception as e:
            print(self.get_last_sql())
            self.conn.rollback()
            raise e
    
    # 执行select 语句
    def select(self, table, fields, condition=None, commit=None, get_first=False, return_iterator=False):
        field_str = self.format_fields(fields)
        if condition:
            select_sql = "SELECT %s FROM `%s` %s" % (field_str, table, condition)
        else:
            select_sql = "SELECT %s FROM `%s`" % (field_str, table)
        
        try:
            self.last_sql_tmp = select_sql
            self.cursor.execute(select_sql)
            if get_first or not return_iterator:
                result = self.__get_execute_result(commit=commit, get_first=get_first)
            else:
                result = self.__get_execute_result_iterator(commit=commit)
            
            return result
        except Exception as e:
            print(self.get_last_sql())
            raise e
    
    # return fetch all:
    def __get_execute_result(self, commit=None, get_first=False):
        result = []
        for item in self.__get_execute_result_iterator(commit=commit, get_first=get_first):
            result.append(item)
        
        if get_first:
            if result:
                return result[0]
            else:
                return result
        else:
            return result
    
    # return iterator(one by one for get data):
    def __get_execute_result_iterator(self, commit=None, get_first=False):
        if get_first:
            yield self.cursor.fetchone()
        else:
            for item in self.cursor:
                yield item
        
        self.__commit(commit)
    
    # fields = []
    # data [{'xx': 'ddd',..}, {}.. ]
    # defaults {'xx': 'default'}
    # batch 批量插入的单次数量
    def insert(self, table, fields, data, defaults={}, batch=1000, commit=None):
        if not data:
            return
        is_many = self.__is_data_many(data)
        insert_field_str = self.format_fields(fields)
        data = self.format_data(fields=fields, data=data, defaults=defaults, is_many=is_many)
        s_string = self.generate_s(fields)
        insert_sql = "INSERT INTO `%s`(%s) VALUES(%s)" % (table, insert_field_str, s_string)
        self.last_sql_tmp = insert_sql
        try:
            if is_many:
                index = 0
                while index < len(data):
                    self.cursor.executemany(insert_sql, data[index:index + batch])
                    index = index + batch
            else:
                self.cursor.execute(insert_sql, data)
            
            self.__commit(commit)
        except Exception as e:
            print(self.get_last_sql())
            self.conn.rollback()
            raise e
    
    # 与insert 不同. 直接根据data中第一个item的key值作为字段
    def insert_by_data(self, table, data, batch=1000, commit=None):
        if not data:
            return
        is_many = self.__is_data_many(data)
        fields = data[0].keys() if is_many else data.keys()
        insert_field_str = self.format_fields(fields)
        data = self.format_data(fields=fields, data=data, defaults={}, is_many=is_many)
        s_string = self.generate_s(fields)
        insert_sql = "INSERT INTO `%s`(%s) VALUES(%s)" % (table, insert_field_str, s_string)
        self.last_sql_tmp = insert_sql
        try:
            if is_many:
                index = 0
                while index < len(data):
                    self.cursor.executemany(insert_sql, data[index:index + batch])
                    index = index + batch
            else:
                self.cursor.execute(insert_sql, data)
            
            self.__commit(commit)
        except Exception as e:
            print(self.get_last_sql())
            self.conn.rollback()
            raise e


if '__main__' == __name__:
    config = {
        'user': 'root',
        'passwd': 'test',
        'host': 'localhost',
        'db': 'test',
        'port': 3306,
        'charset': 'utf8',
    }
