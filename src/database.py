import pymysql
from dotenv import load_dotenv
import os
import log

class Database:
    def __init__(self) -> None:
        load_dotenv()
        
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
            return (connection, connection.cursor())
        except Exception as e:
            log.error(__class__.__name__ , str(e))
    
    def execute(self, sql) -> None:
        connection, cursor = self.connect()
        try:
            cursor.execute(sql)
            connection.commit()
        except Exception as e:
            log.error(__class__.__name__ , str(e))

    def get_all(self, sql) -> list:
        cursor = self.connect()[1]
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        except Exception as e:
            log.error(__class__.__name__ , str(e))
            
    def get(self, sql) -> dict: 
        cursor = self.connect()[1]
        try:
            cursor.execute(sql)
            return cursor.fetchone()
        except Exception as e:
            log.error(__class__.__name__ , str(e))

    def create_tables(self) -> None:
        sql = """
            CREATE TABLE IF NOT EXISTS `author_information` (
                `project_id`                     VARCHAR(100),
                `author`                         VARCHAR(100),
                `project_experience_in_days`     DOUBLE,
                `project_experience_in_hours`    DOUBLE,
                `number_lines_edited`            INT,
                `single_commit`                  INT,
                `amount_commits`                 INT,
                `first_commit`                   VARCHAR(100),
                `last_commit`                    VARCHAR(100),
                `amount_code_smells`             INT,
                `amount_sonar_smells`            INT,
                PRIMARY KEY (`project_id`, `author`),
                FOREIGN KEY(`project_id`) REFERENCES `project_information`(`project_id`)
            );
        """
        sql += """
            CREATE TABLE IF NOT EXISTS `author_percentage_information` (
                `project_id`                      VARCHAR(100),
                `author`                          VARCHAR(100),
                `lines_edited`                    DOUBLE,
                `rounded_lines_edited`            DOUBLE,
                `commits`                         DOUBLE,
                `rounded_commits`                 DOUBLE,
                `experience_in_days`              DOUBLE,
                `rounded_experience_in_days`      DOUBLE,
                `experience_in_hours`             DOUBLE,
                `rounded_experience_in_hours`     DOUBLE,
                `code_smells`                     DOUBLE,
                `rounded_code_smells`             DOUBLE,
                `sonar_smells`                    DOUBLE,
                `rounded_sonar_smells`            DOUBLE,
                FOREIGN KEY(`project_id`, `author`) REFERENCES `author_information`(`project_id`, `author`)
            );
        """
        sql += """
            CREATE TABLE IF NOT EXISTS `project_information` (
                `project_id`            VARCHAR(100) PRIMARY KEY,
                `amount_commits`        INT,
                `first_commit`          VARCHAR(100),
                `last_commit`           VARCHAR(100),
                `total_time_in_days`    DOUBLE,
                `total_time_in_hours`   DOUBLE,
                `number_lines_edited`   INT,
                `amount_code_smells`    INT,
                `amount_sonar_smells`   INT
            );
        """
        return self.execute(sql)