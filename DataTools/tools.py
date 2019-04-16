import pymysql

class MysqlHelper:
    '''python操作mysql的增删改查的封装'''

    def __init__(self, host, user, password, database, port=3306, charset='utf8'):
        '''
        初始化参数
        :param host: 主机
        :param user: 用户名
        :param password: 密码
        :param database: 数据库
        :param port: 端口号，默认是3306
        :param charset: 编码，默认是utf8
        '''
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.charset = charset

    def connect(self):
        '''
        获取连接对象和执行对象
        :return:
        '''
        self.conn = pymysql.connect(host=self.host,
                                    user=self.user,
                                    password=self.password,
                                    database=self.database,
                                    port=self.port,
                                    charset=self.charset)

        self.cur = self.conn.cursor()

    def fetchall(self, sql, params=None):
        '''
        根据sql和参数获取一行数据
        :param sql: sql语句
        :param params: sql语句对象的参数列表，默认值为None
        :return: 查询的所有数据
        '''
        dataall = None
        try:
            count = self.cur.execute(sql, params)
            if count != 0:
                dataall = self.cur.fetchall()
        except Exception as ex:
            print(ex)
        finally:
            # self.close()
            pass
        return dataall

    def fetchone(self, sql, params=None):
        '''
        根据sql和参数获取一行数据
        :param sql: sql语句
        :param params: sql语句对象的参数元组，默认值为None
        :return: 查询的一行数据
        '''
        dataOne = None
        try:
            count = self.cur.execute(sql, params)
            if count != 0:
                dataOne = self.cur.fetchone()
        except Exception as ex:
            print(ex)
        finally:
            # self.close()
            pass
        return dataOne

    def __item(self, sql, params=None):
        '''
        执行增删改
        :param sql: sql语句
        :param params: sql语句对象的参数列表，默认值为None
        :return: 受影响的行数
        '''
        count = 0
        try:
            count = self.cur.execute(sql, params)
            self.conn.commit()
        except Exception as ex:
            print(ex)
        finally:
            # self.close()
            pass
        return count

    def insert(self, sql, params=None):
        '''
        执行新增
        :param sql: sql语句
        :param params: sql语句对象的参数列表，默认值为None
        :return: 受影响的行数
        '''
        return self.__item(sql, params)

    def close(self):
        '''
        关闭执行工具和连接对象
        '''
        if self.cur != None:
            self.cur.close()
        if self.conn != None:
            self.conn.close()
