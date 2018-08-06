from mysql import connector
import os
import threading

class Connection:
    """
    `host` 数据库地址

    `port` 数据库端口

    `user` 用户名

    `password` 密码

    `database` 库名
    """
    host='localhost'
    port=3306
    password='*'
    user='root'
    database=''
    cnt = None
    lock = threading.Lock()

    def __init__(self,host='localhost',port=3306,user='root',password='*',database=''):
        self.host=host
        self.port=port
        self.password=password
        self.user=user
        self.database=database

    def __connect(self):
        try:
            if(self.cnt == None):
                self.cnt = connector.Connect(host=self.host,port=self.port,password=self.password,user=self.user,database=self.database)
                return True
            if self.cnt.is_connected():
                return True
            self.cnt.reconnect()
            return True
        except Exception as e:
            print(e)
            return False

    def insert(self,tableName='',data={}):
        """
        插入行

        `tableName` 表名

        `data` 数据（dict型） `key` 字段名称 `value` 值

        返回 True 或者 False
        """
        if(len(tableName)<=0):
            return False
        if(self.__connect()):
            try:
                cmd = ("INSERT INTO `%s` ("%tableName)
                values = "VALUES ("
                for clumnName in data.keys():
                    cmd += clumnName+','
                    values +="%({0})s,".format(clumnName)
                cmd = cmd[0:-1]+") "+values[0:-1]+")"
                self.lock.acquire()
                cur = self.cnt.cursor()
                cur.execute(cmd,data)
                self.cnt.commit()
                cur.close()
                self.lock.release()
                return True
            except Exception as e:
                print(e)
                self.lock.release()
                return False
        else:
            return False

    def delete(self,tableName='',condition=''):
        """\
        删除行\
\
        `tableName` 表名\
\
        `condition` SQL语句中WHERE部分 例如 "WHERE \`id\` = '1'"\
\
        返回 True 或者 False\
        """
        if(self.__connect()):
            try:
                cmd = 'DELETE FROM {0} {1}'.format(tableName,condition)
                self.lock.acquire()
                cur = self.cnt.cursor()
                cur.execute(cmd)
                self.cnt.commit()
                cur.close()
                self.lock.release()
                return True
            except Exception as e:
                print(e)
                return False
        else:
            return False

    def update(self,tableName='',data={},condition=''):
        """
        更新行

        `tableName` 表名

        `data` 数据（dict型） `key` 字段名称 `value` 值

        `condition` SQL语句中WHERE部分 例如 "WHERE \`id\` = '1'"

        返回 True 或者 False
        """
        if(self.__connect()):
            try:
                values = ""
                for clumnName in data.keys():
                    values +="`{0}`=%({0})s,".format(clumnName)
                values =values[0:-1]
                cmd = 'UPDATE {0} SET {1} {2}'.format(tableName,values,condition)
                self.lock.acquire()
                cur = self.cnt.cursor()
                cur.execute(cmd,data)
                self.cnt.commit()
                cur.close()
                self.lock.release()
                return True
            except Exception as e:
                print(e)
                return False
        else:
            return False
    
    def select(self,tableName='',columns=(),condition=''):
        """
        读取行

        `tableName` 表名

        `columns` 字段名称（元组）

        `condition` SQL语句中WHERE部分 例如 "WHERE \`id\` = '1'"

        返回字典列表 每行为一个字典，中间key是字段名称，value是值
        """
        if(self.__connect()):
            try:
                
                col = '*'
                if(len(columns)>0):
                    col = ''
                    for colName in columns:
                        col += '`%s`,'%colName
                    col = col[0:-1]
                else:
                    cmd = "SELECT `COLUMN_NAME` FROM `information_schema`.`columns` WHERE table_name='{0}' and table_schema='{1}'".format(tableName,self.cnt._database)
                    self.lock.acquire()
                    cur = self.cnt.cursor()
                    cur.execute(cmd)
                    for c in cur.fetchall():
                        columns += c
                    cur.close()
                    self.lock.release()
                cmd = 'SELECT {0} FROM `{1}` {2}'.format(col,tableName,condition)
                self.lock.acquire()
                cur = self.cnt.cursor()
                cur.execute(cmd)
                datalist = []
                for row in cur:
                    di = {}
                    for i in range(len(columns)):
                        di[columns[i]] = row[i]
                    datalist.append(di)
                cur.close()
                self.lock.release()
                return datalist
            except Exception as e:
                print(e)
                return []
                self.lock.release()
        else:
            return []

    def buildModel(self,filePath):
        if not os.path.exists(filePath):
            os.makedirs(filePath)
        if self.__connect():
            cur = self.cnt.cursor()
            cmd = "SELECT `table_name`,`COLUMN_NAME`,`DATA_TYPE`,`COLUMN_KEY`,`EXTRA` FROM `information_schema`.`columns` WHERE table_schema='{0}'".format(self.cnt._database)
            cur.execute(cmd)
            tabelModel = {}
            for row in cur:
                if row[0] in tabelModel:
                    tabelModel[row[0]] += ((row[1],row[2],row[3],row[4]),)
                else:
                    tabelModel[row[0]] = ()
                    tabelModel[row[0]] += ((row[1],row[2],row[3],row[4]),)
            cur.close()
            for table in tabelModel.items():
                f=open('%s\\%s.py'%(filePath,table[0]), 'w')
                f.write("from MySQLTools.MySqlTools import Model\nfrom MySQLTools.MySqlTools import Connection\nimport datetime\n")
                pri = ''
                ai = ''
                defalt = ''
                for column in table[1]:
                    f.write("{0} = '{0}'\n".format(column[0]))
                    if column[2] == 'PRI':
                        pri += '%s, ' % column[0]
                    if column[3] == 'auto_increment':
                        ai = column[0]
                    ctype = column[1]
                    if (ctype == 'bigint'
                    or ctype == 'decimal'
                    or ctype =='double'
                    or ctype == 'enum'
                    or ctype == 'int'
                    or ctype == 'smallint'
                    or ctype == 'tinyint'):
                        defalt += '%s : 0, ' % column[0]
                    elif (ctype == 'blob'
                    or ctype == 'longblob'):
                        defalt += "%s : b'', " % column[0]
                    elif (ctype == 'char'
                    or ctype == 'longtext'
                    or ctype == 'mediumtext'
                    or ctype == 'text'
                    or ctype == 'varchar'):
                        defalt += "%s : '', " % column[0]
                    elif (ctype == 'datetime'
                    or ctype == 'timestamp'):
                        defalt += "%s : datetime.datetime(datetime.MINYEAR,1,1,0,0,1), " % column[0]
                    else:
                        print('Undefind data type "%s"' % column[0])
                        defalt += "%s : None, " % column[0]
                defalt = defalt[0:-2]
                pri = pri[0:-1]
                f.write('class %s(Model):\n' % table[0])
                f.write('\tdef __new__(cls,connection,args = None):\n')
                f.write("\t\treturn super().__new__(cls,connection,((%s),'%s','%s'))\n\n" % (pri,table[0],ai))
                f.write("\tdef __init__(self,connection,content=None):\n")
                f.write("\t\tif content is None:\n")
                f.write("\t\t\tsuper().__init__({%s})\n" % defalt)
                f.write("\t\telse:\n")
                f.write("\t\t\tsuper().__init__(content)\n")
                f.close()

class Model(dict):
    tableName = ''
    key = ()
    aicolumn = ''
    conn = None
    
    def __init__(self,content):
        for each in content.items():
            self[each[0]] = each[1]

    def __new__(cls,connection,args):
        cls.conn = connection
        cls.key = args[0]
        cls.tableName = args[1]
        cls.aicolumn = args[2]
        return dict.__new__(cls)

    @classmethod
    def findAll(cls,condition):
        data = cls.conn.select(cls.tableName,condition = condition)
        result = []
        for each in data:
            result.append(cls(cls.conn,each))
        return result

    @classmethod
    def find(cls,pk):
        conditionStr = "WHERE "
        keyLen = len(cls.key)
        if len(pk)!=keyLen:
            print("pk length dosen't match!!!")
            return None
        conditionStr += "`{0}`='{1}'".format(cls.key[0],pk[0])
        if keyLen>1:
            for i in range(len(cls.key)-1):
                conditionStr += " AND `{0}`='{1}'".format(cls.key[i+1],pk[i+1])
        data = cls.conn.select(cls.tableName,condition = conditionStr)
        if len(data)>0:
            return cls(cls.conn,data[0])
        return None
    
    def save(self,ai = True):
        if ai:
            if len(self.aicolumn)>0:
                del self[self.aicolumn]
        self.conn.insert(self.tableName,self)
    
    def update(self):
        conditionStr = "WHERE `{0}`='{1}'".format(self.key[0],self.pop(self.key[0]))
        keyLen = len(self.key)
        if keyLen>1:
            for i in range(len(self.key)-1):
                conditionStr += " AND `{0}`='{1}'".format(self.key[i+1],self.pop(self.key[i+1]))
        self.conn.update(self.tableName,self,conditionStr)




def main():
    connection = Connection(input("Host Name:"),int(input("Port Number:")),input("User Name:"),input("Password:"),input("Database Name:"))
    connection.buildModel(input("Path To Build Model:"))

if __name__ == '__main__':
    main()