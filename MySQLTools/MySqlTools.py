from mysql import connector
import os
import threading

class Connection:
    r"""
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

    def insert(self,tableName,data,ai = False):
        """
        插入行

        `tableName` 表名

        `data` 数据（dict型） `key` 字段名称 `value` 值

        `ai` 是否查询最近自增键值

        返回 (True 或者 False , 自增键值)
        """
        if(len(tableName)<=0)or(len(data)<=0):
            return False,-1
        #检查链接
        if(self.__connect()):
            aivalue = -1
            #拼接SQL语句
            cmd = ("INSERT INTO `%s` ("%tableName)
            values = "VALUES ("
            for clumnName in data.keys():
                cmd += clumnName+','
                values +="%({0})s,".format(clumnName)
            cmd = cmd[0:-1]+") "+values[0:-1]+")"
            try:
                #获取线程锁，执行SQL语句
                self.lock.acquire()
                cur = self.cnt.cursor()
                cur.execute(cmd,data)
                self.cnt.commit()
                cur.close()
                if ai:
                    #如果需要获取自增键就在解锁线程锁之前获取，能保证唯一
                    cur = self.cnt.cursor()
                    cur.execute('SELECT LAST_INSERT_ID()')
                    aivalue = cur.fetchone()[0]
                    cur.close()
                self.lock.release()
                return True,aivalue
            except Exception as e:
                print(e)
                self.lock.release()
                return False,aivalue
        else:
            return False,-1

    def delete(self,tableName,condition):
        r"""
        删除行

        `tableName` 表名

        `condition` SQL语句中WHERE部分 例如 "WHERE \`id\` = '1'"

        返回 True 或者 False
        """
        if(len(tableName)<=0)or(len(condition)<=0):
            return False
        #检查链接
        if(self.__connect()):
            #拼接SQL语句
            cmd = 'DELETE FROM {0} {1}'.format(tableName,condition)
            try:
                #获取线程锁，执行SQL语句
                self.lock.acquire()
                cur = self.cnt.cursor()
                cur.execute(cmd)
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

    def update(self,tableName,data,condition=''):
        r"""
        更新行

        `tableName` 表名

        `data` 数据（dict型） `key` 字段名称 `value` 值

        `condition` SQL语句中WHERE部分 例如 "WHERE \`id\` = '1'"

        返回 True 或者 False
        """
        if(len(tableName)<=0)or(len(data)<=0):
            return False
        #检查链接
        if(self.__connect()):
            #拼接SQL语句
            values = ""
            for clumnName in data.keys():
                values +="`{0}`=%({0})s,".format(clumnName)
            values =values[0:-1]
            cmd = 'UPDATE {0} SET {1} {2}'.format(tableName,values,condition)
            try:
                #获取线程锁，执行SQL语句
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
    
    def select(self,tableName,columns=(),condition=''):
        r"""
        读取行

        `tableName` 表名

        `columns` 字段名称（元组）

        `condition` SQL语句中WHERE部分 例如 "WHERE \`id\` = '1'"

        返回字典列表 每行为一个字典，中间key是字段名称，value是值
        """
        if(len(tableName)<=0):
            return False
        #检查链接
        if(self.__connect()):
            #拼接SQL语句
            col = '*'
            if(len(columns)>0):
                #如果有限定列就按限定列拼接
                col = ''
                for colName in columns:
                    col += '`%s`,'%colName
                col = col[0:-1]
            else:
                #如果没有限定列，先获取所有列
                cmd = "SELECT `COLUMN_NAME` FROM `information_schema`.`columns` WHERE table_name='{0}' and table_schema='{1}'".format(tableName,self.cnt._database)
                try:
                    self.lock.acquire()
                    cur = self.cnt.cursor()
                    cur.execute(cmd)
                    for c in cur.fetchall():
                        columns += c
                    cur.close()
                    self.lock.release()
                except Exception as e:
                    print(e)
                    self.lock.release()
                    return []
            cmd = 'SELECT {0} FROM `{1}` {2}'.format(col,tableName,condition)
            try:
                #获取线程锁，执行SQL语句
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
                self.lock.release()
                return []
        else:
            return []

    def buildModel(self,filePath):
        r"""

        `filePath` 创建地址

        创建Model代码
        """
        print('Building Start!')
        print('Building file path...')
        #检查路径是否存在
        if not os.path.exists(filePath):
            os.makedirs(filePath)
        print('Checking connection...')
        #检查链接
        if self.__connect():
            print('Ececuting SQL command...')
            #获取信息
            cur = self.cnt.cursor()
            cmd = "SELECT `table_name`,`COLUMN_NAME`,`DATA_TYPE`,`COLUMN_KEY`,`EXTRA` FROM `information_schema`.`columns` WHERE table_schema='{0}'".format(self.cnt._database)
            cur.execute(cmd)
            tabelModel = {}
            print('Getting table info...')
            #分析信息，同一个表下的列放一起
            for row in cur:
                if row[0] in tabelModel:
                    tabelModel[row[0]] += ((row[1],row[2],row[3],row[4]),)
                else:
                    tabelModel[row[0]] = ()
                    tabelModel[row[0]] += ((row[1],row[2],row[3],row[4]),)
            cur.close()
            print('Building code...')
            #开始组建代码
            #遍历每个表
            for table in tabelModel.items():
                #table[0]是表名 table[1]是表内列信息
                print('Building %s model...'%table[0])
                f=open('%s\\%s.py'%(filePath,table[0]), 'w')
                #import一类
                f.write("import sys\nimport os\nsys.path.append(os.path.dirname(os.path.dirname(__file__)))\nfrom MySQLTools.MySqlTools import Model\nfrom MySQLTools.MySqlTools import Connection\nimport datetime\n\n")
                pri = ''
                ai = ''
                defalt = ''
                allcol = ''
                #遍历每个列的信息，pri是主键，ai是自增列，defalt是设定默认值，allcol是所有列
                for column in table[1]:
                    f.write("{0} = '{0}'\n".format(column[0]))
                    allcol += '%s, ' % column[0]
                    if column[2] == 'PRI':
                        pri += '%s, ' % column[0]
                    if column[3] == 'auto_increment':
                        ai = column[0]
                    ctype = column[1]
                    #相当于按类型switch
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
                allcol = allcol[0:-1]
                #将遍历了列的信息写在继承了Model的类里
                f.write('\nclass %s(Model):\n\n' % table[0])
                f.write('\tdef __new__(cls,connection,args = None):\n')
                f.write("\t\treturn super().__new__(cls,connection,((%s),'%s','%s',(%s)))\n\n" % (pri,table[0],ai,allcol))
                f.write("\tdef __init__(self,connection,content=None):\n")
                f.write("\t\tif content is None:\n")
                f.write("\t\t\tsuper().__init__({%s})\n" % defalt)
                f.write("\t\telse:\n")
                f.write("\t\t\tsuper().__init__(content)\n")
                f.close()
            print('Build Successed!!')

class Model(dict):
    tableName = ''
    key = ()
    aicolumn = ''
    conn = None
    columns = ()

    def __init__(self,content):
        for each in content.items():
            self[each[0]] = each[1]

    def __new__(cls,connection,args):
        cls.conn = connection
        cls.key = args[0]
        cls.tableName = args[1]
        cls.aicolumn = args[2]
        cls.columns = args[3]
        return dict.__new__(cls)

    @classmethod
    def findAll(cls,condition=''):
        r"""

        `condition` SQL语句中WHERE部分 例如 "WHERE \`id\` = '1'"

        返回 Model 的实例列表
        """
        #获取表内信息
        data = cls.conn.select(cls.tableName,cls.columns,condition)
        result = []
        for each in data:
            #每个都弄一个实例
            result.append(cls(cls.conn,each))
        return result

    @classmethod
    def find(cls,pk):
        r"""

        `pk` 查询主键对应的实例

        返回 Model 的实例
        """
        conditionStr = "WHERE "
        keyLen = len(cls.key)
        #判断主键数量
        if len(pk)!=keyLen:
            print("pk length dosen't match!!!")
            return None
        ppk = ()
        #替换参数中的'\'和'''防止造成SQL语句出错
        for i in range(keyLen):
            ppk += ('{0}'.format(pk[i]).replace('\\','\\\\').replace("'","\\'"),)
        #按照主键拼个搜主键的where语句
        conditionStr += "`{0}`='{1}'".format(cls.key[0],ppk[0])
        #主键大于一个的时候后面使用AND连起来
        if keyLen>1:
            for i in range(len(cls.key)-1):
                conditionStr += " AND `{0}`='{1}'".format(cls.key[i+1],ppk[i+1])
        data = cls.conn.select(cls.tableName,cls.columns,conditionStr)
        #找得到就创建实例
        if len(data)>0:
            return cls(cls.conn,data[0])
        return None
    
    def save(self):
        r"""

        保存实例进数据库
        """
        ai = False
        #如果有自增键就获取自增键
        if (len(self.aicolumn)>0):
            if (self.aicolumn in self):
                del self[self.aicolumn]
            ai = True
        #创建一行
        result,aivalue = self.conn.insert(self.tableName,self,ai)
        if ai:
            self[self.aicolumn] = aivalue
        return result
    
    def update(self):
        r"""

        数据库中对应的数据
        """
        #检查主键是不是全的
        for pk in self.key:
            if not (pk in self):
                print('%s has no value'%pk)
                return False
        popkey = ()
        #先把主键弹出来，不更新
        for pk in self.key:
            popkey += (self.pop(pk),)
        #按照主键产生选择条件
        conditionStr = "WHERE `{0}`='{1}'".format(self.key[0],popkey[0])
        keyLen = len(self.key)
        #主键大于一个的时候后面使用AND连起来
        if keyLen>1:
            for i in range(keyLen-1):
                conditionStr += " AND `{0}`='{1}'".format(self.key[i+1],popkey[i+1])
        #更新
        result = self.conn.update(self.tableName,self,conditionStr)
        #把主键加回来
        for i in range(keyLen):
            self[self.key[i]] = popkey[i]
        return result

def main():
    try:
        connection = Connection(input("Host Name:"),int(input("Port Number:")),input("User Name:"),input("Password:"),input("Database Name:"))
        connection.buildModel(os.path.dirname(os.path.dirname(__file__))+'\\Model')
    except Exception as e:
        print(e)

if __name__ == '__main__':
    main()