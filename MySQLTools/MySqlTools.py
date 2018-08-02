from mysql import connector
import os

class MySqlTools:
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
                cur = self.cnt.cursor()
                cmd = ("INSERT INTO `%s` ("%tableName)
                values = "VALUES ("
                for clumnName in data.keys():
                    cmd += clumnName+','
                    values +="%({0})s,".format(clumnName)
                cmd = cmd[0:-1]+") "+values[0:-1]+")"
                cur.execute(cmd,data)
                self.cnt.commit()
                cur.close()
                self.cnt.close()
                return True
            except Exception as e:
                print(e)
                return False
        else:
            return False

    def delete(self,tableName='',condition=''):
        """
        删除行

        `tableName` 表名

        `condition` SQL语句中WHERE部分 例如 "WHERE \`id\` = '1'"

        返回 True 或者 False
        """
        if(self.__connect()):
            try:
                cur = self.cnt.cursor()
                cmd = 'DELETE FROM {0} {1}'.format(tableName,condition)
                cur.execute(cmd)
                self.cnt.commit()
                cur.close()
                self.cnt.close()
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
                cur = self.cnt.cursor()
                values = ""
                for clumnName in data.keys():
                    values +="`{0}`=%({0})s,".format(clumnName)
                values =values[0:-1]
                cmd = 'UPDATE {0} SET {1} {2}'.format(tableName,values,condition)
                cur.execute(cmd,data)
                self.cnt.commit()
                cur.close()
                self.cnt.close()
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
                cur = self.cnt.cursor()
                col = '*'
                if(len(columns)>0):
                    col = ''
                    for colName in columns:
                        col += '`%s`,'%colName
                    col = col[0:-1]
                else:
                    cmd = "SELECT `COLUMN_NAME` FROM `information_schema`.`columns` WHERE table_name='{0}' and table_schema='{1}'".format(tableName,self.cnt._database)
                    cur.execute(cmd)
                    for c in cur.fetchall():
                        columns += c
                    cur.close()
                    cur = self.cnt.cursor()
                cmd = 'SELECT {0} FROM `{1}` {2}'.format(col,tableName,condition)
                cur.execute(cmd)
                datalist = []
                for row in cur:
                    di = {}
                    for i in range(len(columns)):
                        di[columns[i]] = row[i]
                    datalist.append(di)
                cur.close()
                self.cnt.close()
                return datalist
            except Exception as e:
                print(e)
                return []
        else:
            return []

    def buildModel(self,filePath):
        if not os.path.exists(filePath):
            os.makedirs(filePath)
        if self.__connect():
            cur = self.cnt.cursor()
            cmd = "SELECT `table_name`,`COLUMN_NAME` FROM `information_schema`.`columns` WHERE table_schema='{0}'".format(self.cnt._database)
            cur.execute(cmd)
            model = {}
            for row in cur:
                if row[0] in model:
                    model[row[0]] += (row[1],)
                else:
                    model[row[0]] = ()
                    model[row[0]] += (row[1],)
            for table in model.items():
                f=open('%s\\%s.py'%(filePath,table[0]), 'w')
                for column in table[1]:
                    f.write("{0} = '{0}'\n".format(column))
                f.close()


def main():
    connection = MySqlTools(input("Host Name:"),int(input("Port Number:")),input("User Name:"),input("Password:"),input("Database Name:"))
    connection.buildModel(input("Path To Build Model:"))
    input("Input Anything To Close:")

if __name__ == '__main__':
    main()