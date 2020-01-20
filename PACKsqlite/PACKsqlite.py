# conding:utf-8
# AUTHOR: as_asn
#   DATE: 2020/01/17 周五
import sqlite3
import sys
import re


class MyDbase(object):
    """
    我的操作类
    轻数据库sqlite的常用操作方法封装
    """

    def __init__(self, fname_db):
        """
        方法：初始化
        初始化数据库的连接，建立游标对象
        """
        self.obj_connection = sqlite3.connect(fname_db)
        self.obj_cursor = self.obj_connection.cursor()

    def getTables(self):
        """
        方法：获取表名列表
        """
        str_sql = '''SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;'''
        self.runSql(str_sql)
        rets = self.obj_cursor.fetchall()
        names_table = list()
        for t in rets:
            names_table.append(t[0])
        return names_table

    def getTableInfos(self, name_table, point=None):
        """
        方法：获取table_info列表
        """
        str_sql = '''PRAGMA table_info(%s);''' % name_table
        self.runSql(str_sql)
        rets = self.obj_cursor.fetchall()
        infos_table = list()
        if point:
            for rs in rets:
                infos_table.append(rs[point])
        else:
            infos_table = rets
        return infos_table

    def getFieldNames(self, name_table):
        """
        方法：获取字段名列表
        """
        names_field = self.getTableInfos(name_table, point=1)
        return names_field

    def getFieldPks(self, name_table):
        """
        方法：获取pk值列表
        """
        pks = self.getTableInfos(name_table, point=-1)
        return pks

    def findTable(self, name_table):
        """
        方法：发现表
        输入表名，返回是否存在的结果
        """
        names_table = self.getTables()
        if name_table in names_table:
            is_exist = True
        else:
            is_exist = False
        return is_exist

    def findField(self, name_table, name_field):
        """
        方法：发现字段
        输入字段名，返回是否存在的结果
        """
        names_field = self.getFieldNames(name_table)
        if name_field in names_field:
            is_exist = True
        else:
            is_exist = False
        return is_exist

    def toDict(self, **kwargs):
        """
        方法：输入参数转换字典
        """
        return kwargs

    def searchByDict(self, name_table, dict_for_where):
        """
        方法：用toDict生成的限制条件搜寻内容
        """
        names_field = self.getFieldNames(name_table)
        str_for_where = ""
        for f in names_field:
            if f in dict_for_where:
                str_for_where += '''%s="%s" and ''' % (f, dict_for_where[f])
        str_sql = '''SELECT * FROM %s WHERE %s;''' % (
            name_table, str_for_where[:-5])
        self.runSql(str_sql)
        rets = self.obj_cursor.fetchall()
        return rets

    def newTable(self, name_table):
        """
        方法：建立新表
        """
        if self.findTable(name_table):
            print("表[%s]已存在" % name_table)
        else:
            str_sql = '''CREATE TABLE %s (id integer PRIMARY KEY DEFAULT "");''' % name_table
            self.runSql(str_sql)
            print("新建表[%s]" % name_table)
        return name_table

    def newField(self, name_table, name_field):
        """
        方法：新建字段
        name_table：表名
        name_field：字段名
        """
        if self.findField(name_table, name_field):
            print("字段[%s]已存在" % name_field)
        else:
            str_sql = '''alter table %s add %s text default ""''' % (
                name_table, name_field)
            self.runSql(str_sql)
            print("新建字段[%s]" % name_field)
        return name_field

    def getContents(self, name_table):
        """
        方法：获取整张表的内容
        返回：[(a0, b0, c0), (a1, b1, c1)]
        """
        str_sql = '''SELECT * FROM %s;''' % name_table
        self.runSql(str_sql)
        contents = self.obj_cursor.fetchall()
        return contents

    def __maxRows(self, field_p, name_table):
        str_sql = '''SELECT MAX(%s) FROM %s;''' % (field_p, name_table)
        self.runSql(str_sql)
        rets = self.obj_cursor.fetchall()
        nun_count = rets[0][0]
        return nun_count

    def updateRow(self, name_table, dict_for_where, **kwargs):
        """
        方法：更新行内容
        用指定的条件更新行内容
        返回：更改后的内容列表
        """
        names_field = self.getFieldNames(name_table)
        str_for_where = ""
        str_splice = ""
        for f in names_field:
            if f in dict_for_where:
                # t = type(dict_for_where[f])
                # if t is int:
                #     v = '''%s''' % dict_for_where[f]
                # else:
                #     v = '''"%s"''' % dict_for_where[f]
                v = '''%s''' % dict_for_where[f]
                str_for_where += '''%s="%s" and ''' % (f, v)
            if f in kwargs:
                v = '''%s''' % kwargs[f]
                str_splice += '''%s="%s", ''' % (f, v)
        str_sql = '''UPDATE %s SET %s WHERE %s;''' % (
            name_table, str_splice[:-2], str_for_where[:-5])
        self.runSql(str_sql)

    def insertRow(self, name_table, **kwargs):
        """
        方法：插入新行
        只指定表名，不指定字段时，添加空行
        返回：插入后的内容列表
        """
        names_field = self.getFieldNames(name_table)
        pks = self.getFieldPks(name_table)
        str_fields = ""
        str_values = ""
        temps = dict()
        for f, pk in zip(names_field, pks):
            str_fields += ('''%s, ''' % f)
            if pk == 1:  # 是主键的情形
                v = "null"
                value_id = self.__maxRows(f, name_table) + 1
                pkey = f
            else:
                v = '''""'''
            if f in kwargs:  # 如果存在输入，则覆盖之前的赋值
                v = '''"%s"''' % kwargs[f]
                if pk == 1:  # 是主键的情形
                    value_id = kwargs[f]
            str_values += ('''%s, ''' % v)
            temps[f] = v
        str_sql = '''INSERT INTO %s (%s) VALUES (%s);''' % (
            name_table, str_fields[:-2],  str_values[:-2])
        ret = self.runSql(str_sql)
        if ret:
            str_for_back = "%s=%s" % (pkey, value_id)
            str_sql = '''SELECT * FROM %s WHERE %s;''' % (
                name_table, str_for_back)
            ret = self.runSql(str_sql)
            rets = self.obj_cursor.fetchall()
            return rets

    def delByDict(self, name_table, dict_for_where):
        """
        方法：删除指定行
        """
        names_field = self.getFieldNames(name_table)
        str_for_where = ""
        for f in names_field:
            if f in dict_for_where:
                str_for_where += '''%s="%s" and ''' % (f, dict_for_where[f])
        str_sql = '''DELETE FROM %s WHERE %s;''' % (
            name_table, str_for_where[:-5])
        self.runSql(str_sql)

    def findByDict(self, name_table, dict_for_where):
        """
        方法：找到指定行（精准）
        """
        names_field = self.getFieldNames(name_table)
        str_for_where = ""
        for f in names_field:
            if f in dict_for_where:
                str_for_where += '''%s="%s" and ''' % (f, dict_for_where[f])
        str_sql = '''SELECT * FROM %s WHERE %s;''' % (
            name_table, str_for_where[:-5])
        self.runSql(str_sql)
        rets = self.obj_cursor.fetchall()
        return rets

    def searchByStrings(self, name_table, str_for_where):
        """
        方法：按照指定内容进行查询（模糊）
        """
        strings = self.__toStrings(str_for_where)
        names_field = self.getFieldNames(name_table)
        str_for_where = ""
        for f in names_field:
            for s in strings:
                str_for_where += '''%s like "%%%s%%" or ''' % (f, s)
        str_sql = '''SELECT * FROM %s WHERE %s;''' % (
            name_table, str_for_where[:-4])
        self.runSql(str_sql)
        rets = self.obj_cursor.fetchall()
        return rets

    def __toStrings(self, string):
        """
        方法：字符串转化列表
        输入特定格式的字符串，转化为供给searchByStrings方法使用的格式化列表
        """
        strings = list()
        list_temp = ["[(](.*?)[)]", "\"(.*?)\"", "\'(.*?)\'", ]
        for t in list_temp:
            rule = re.compile(t)
            rets = rule.findall(string)
            string = rule.sub("", string)
            strings.extend(rets)
        rets = re.split(" ", string)
        strings.extend(rets)
        strings = list(set(strings))
        if "" in strings:
            strings.remove("")
        return strings

    def runSqlByParm(self, str_sql, *args):
        """
        方法：执行SQL语句（参数化）
        """
        try:
            self.obj_cursor.execute(str_sql, args)
            print("执行语句：%s" % str_sql)
            ret = True
        except Exception as e:
            print("错误语句：%s" % str_sql)
            print("错误信息：%s\n执行语句：%s" % (e, str_sql))
            ret = False
        return ret

    def runSql(self, str_sql):
        """
        方法：执行SQL语句
        """
        try:
            self.obj_cursor.execute(str_sql)
            print("执行语句：%s" % str_sql)
            ret = True
        except Exception as e:
            print("错误语句：%s" % str_sql)
            print("错误信息：%s\n执行语句：%s" % (e, str_sql))
            ret = False
        return ret

    def __del__(self):
        """
        方法：退出
        退出前关闭以及断开连接
        """
        print("------%s.%s------" %
              (type(self).__name__, sys._getframe().f_code.co_name))
        self.obj_connection.commit()
        self.obj_connection.close()
        print("------%s------" % "提交并关闭数据库")


if __name__ == "__main__":
    op = MyDbase("mem2.db")
    parm = op.toDict(id=2)
    ret = op.searchByStrings("测试表", """3 4 5""")
    print(ret)
    
