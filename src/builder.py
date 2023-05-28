import pymysql
from dotenv import load_dotenv
import os
import log
from datetime import datetime

class DB:
    
    def __init__(self, table_name:str) -> 'DB':
        load_dotenv()
        self.table_name = table_name
        self.sql = ""
        
    def connect(self) -> tuple:
        try:
            config = {
                'user': os.getenv('DB_USERNAME'),
                'password': os.getenv('DB_PASSWORD'),
                'host': os.getenv('DB_HOST'),
                'database': os.getenv('DB_DATABASE'),
                'charset': 'utf8mb4'
            }
            connection = pymysql.connect(**config, cursorclass=pymysql.cursors.DictCursor)
            (connection, connection.cursor())
        except Exception as e:
            log.error("DB:connect:", str(e))

    def where_equal(self, column:str, value:any) -> 'DB':
        return self.where(column, "=", value)

    def where(self, column:str, operator:str, value:any) -> 'DB':
        if("WHERE" in self.sql):
            self.sql += f" AND {column} {operator} '{value}'"
        else:
            self.sql += f"WHERE {column} {operator} '{value}'"
        return self

    def where_date(self, column:str, operator:str, value:str, date_format:str = '%Y-%m-%d %H:%M:%S') -> 'DB':
        date = datetime.strptime(value, date_format)
        format_date = date.strftime("%Y-%m-%d %H:%M:%S")
        if("WHERE" in self.sql):
            self.sql += f" AND {column} {operator} '{format_date}'"
        else:
            self.sql += f"WHERE {column} {operator} '{format_date}'"
        return self
    
    def where_null(self, column:str) -> 'DB':
        if("WHERE" in self.sql):
            self.sql += f" AND {column} IS NULL"
        else:
            self.sql += f"WHERE {column} IS NULL"
        return self
    
    def where_not_null(self, column:str) -> 'DB':
        if("WHERE" in self.sql):
            self.sql += f" AND {column} IS NOT NULL"
        else:
            self.sql += f"WHERE {column} IS NOT NULL"
        return self
    
    def group_by(self, column:str|list) -> 'DB':
        group = column
        if(type(column) == list):
            group = ""
            for key in column:
                if(group != ""):
                    group += ", " + key
                else:
                    group += key

        self.sql += f" GROUP BY {group}"
        return self

    def select(self, columns:list = None) -> 'DB':
        self.prepare_select(columns)
        return self

    def prepare_select(self, columns:list=None) -> str:
        select = ""
        if(columns != None):
            for key in columns:
                if(select != ""):
                    select += f",{key} "
                else:
                    select += f"{key}"

        if(select == ""):
            select = " * "
        if("SELECT" in self.sql):
            self.sql = self.sql[self.sql.find(self.table_name):].replace(f"{self.table_name} ", "")
        self.sql = f"SELECT {select} FROM {self.table_name} {self.sql}"
        return self.sql

    def update_one(self, column:str, value:str|int|float) -> bool:
        sql = self.sql
        self.sql = f"UPDATE {self.table_name} SET {column} = '{value}'"
        if("AND" in sql):
            self.sql += " WHERE " + sql
        self.execute()

    def update(self, data:dict) -> bool:
        update = ""
        for key, value in data.items():
            if(update != ""):
                update += f",{key} = '{value}' "
            else:
                update += f"{key} = '{value}'"
        
        sql = self.sql
        self.sql = f"UPDATE {self.table_name} SET {update}"
        if("AND" in sql):
            self.sql += " WHERE " + sql
        self.execute()

    def execute(self, sql:str = "") -> bool:
        try:
            if(sql == ""):
                sql = self.sql
            log.debug("DB:execute", f" SQL -> {sql}")
            connection, cursor = self.connect()
            cursor.execute(sql)
            connection.commit()
            return True
        except Exception as e:
            log.error("DB:execute", str(e))
            return False

    def get(self, columns=None) -> list:
        try:
            sql = self.prepare_select(columns)
            log.debug("DB:get", f" SQL -> {sql}")
            # cursor = self.connect()[1]
            # cursor.execute(sql)
            # return cursor.fetchall()
            return []
        except Exception as e:
            log.error("DB:get", str(e))
            
    def first(self) -> dict:
        try:
            self.prepare_select()
            log.debug("DB:first", f" SQL -> {self.sql}")
            cursor = self.connect()[1]
            cursor.execute(self.sql)
            return cursor.fetchone()
        except Exception as e:
            log.error("DB:first", str(e))
            
if __name__ == "__main__":
    author_info = DB("author_information")
    author_info = author_info.where("code_smell", ">=", "100").where("code_smell", "<=", "200").group_by(["author", "project_id"])
    response = author_info.get(['code_smells'])

    print(response, author_info.where_date("created_at", ">=", "2020-01-01", "%Y-%m-%d").get(['lines_edited']))
    print(DB.execute("SELECT * FROM author_information WHERE code_smell >= 100 AND code_smell <= 200 GROUP BY author, project_id"))