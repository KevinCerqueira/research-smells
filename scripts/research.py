import os
from datetime import datetime
import sqlite3

class Research:

    conn_data_set = None
    data_set = None

    conn_local_db = None
    local_db = None

    def __init__(self, fast: bool = False):
        # Path do script
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))

        # Conectando ao banco de dados do https://github.com/clowee/The-Technical-Debt-Data_set/
        path_data_set = os.path.join(BASE_DIR, "dataset.db")
        self.conn_data_set = sqlite3.connect(path_data_set)
        self.data_set = self.conn_data_set.cursor()
        
        # Conectando ao banco local
        path_local_db = os.path.join(BASE_DIR, "research.db")
        self.conn_local_db = sqlite3.connect(path_local_db)
        self.local_db = self.conn_local_db.cursor()

        # Caso deseje pular a etapa de verificação de leitura e gravação dos projetos e autores 
        if (not fast):
            self.init_local_table()

    # Fecha as conexões
    def close_connections(self):
        self.data_set.close()
        self.local_db.close()

    # Cria a tabela que é armazendo os dados caso não exista
    def init_local_table(self):
        self.local_db.execute("""
                CREATE TABLE IF NOT EXISTS "author_information" (
                "project_id"                  TEXT,
                "author"	                  TEXT,
                "project_experience_in_days"  REAL,
                "project_experience_in_hours" REAL,
                "number_lines_edited"	      INTEGER,
                "single_commit"               INTEGER,
                "amount_commits"		      INTEGER,
                "first_commit"                TEXT,
                "last_commit"                 TEXT,
                "amount_code_smells"	      INTEGER,
                "amount_sonar_smells"	      INTEGER
                );
            """)
        self.local_db.execute("""
                CREATE TABLE IF NOT EXISTS "project_information" (
                "project_id"            TEXT,
                "amount_commits"		INTEGER,
                "first_commit"          TEXT,
                "last_commit"           TEXT,
                "total_time_in_days"    TEXT,
                "total_time_in_hours"   TEXT,
                "number_lines_edited"	INTEGER,
                "amount_code_smells"	INTEGER,
                "amount_sonar_smells"	INTEGER
                );
            """)
        self.conn_local_db.commit()
        self.insert_authors_and_projects()

    # Insere todos os projetos e autores no banco de dados local
    def insert_authors_and_projects(self):
        self.data_set.execute("SELECT project_id, author FROM git_commits")

        for result in self.data_set.fetchall():
            project_id = result[0]
            author = result[1]
            # Inserindo os projetos e os autores na tabela que guarda as informações dos autores
            self.local_db.execute("SELECT 1 FROM author_information WHERE project_id = ? AND author = ?", (project_id, author))
            if (len(self.local_db.fetchall()) == 0):
                self.local_db.execute("INSERT INTO author_information (project_id, author) VALUES (?,?)", (project_id, author))

            # Inserindo os projetos na tabela que guarda as informações dos projetos
            self.local_db.execute("SELECT 1 FROM project_information WHERE project_id = ?", (project_id,))
            if (len(self.local_db.fetchall()) == 0):
                self.local_db.execute("INSERT INTO project_information (project_id) VALUES (?)", (project_id,))

        self.conn_local_db.commit()

    # Lê o Data Set e grava no banco local a quantidade de sonar smells para cada dev
    def read_amout_sonar_smells_author(self):
        self.data_set.execute("""
            SELECT
                COUNT(DISTINCT sonar_analysis.revision) AS amount_sonar_smells,
                git_commits.project_id,
                git_commits.author
            FROM git_commits
            INNER JOIN
                git_commits_changes ON git_commits.commit_hash = git_commits_changes.commit_hash
            INNER JOIN 
                sonar_analysis ON git_commits.commit_hash = sonar_analysis.revision
            INNER JOIN 
                sonar_issues ON sonar_analysis.analysis_key = sonar_issues.creation_analysis_key
            WHERE
                git_commits.merge = 'False' 
                AND sonar_issues.type = 'CODE_SMELL' 
            GROUP BY git_commits.author
        """)

        for result in self.data_set.fetchall():
            print("Updating amout sonar smells to: ", result)
            self.local_db.execute(
                """
                    UPDATE 
                        author_information 
                    SET 
                        amount_sonar_smells = ?
                    WHERE 
                        project_id = ? 
                        AND author = ?
                """,
                (result)
            )
        self.conn_local_db.commit()
        
    # Lê o Data Set e grava no banco local a quantidade de sonar smells para cada projeto
    def read_amout_sonar_smells_project(self):
        self.data_set.execute("""
            SELECT
                COUNT(DISTINCT sonar_analysis.revision) AS amount_sonar_smells,
                git_commits.project_id
            FROM git_commits
            INNER JOIN
                git_commits_changes ON git_commits.commit_hash = git_commits_changes.commit_hash
            INNER JOIN 
                sonar_analysis ON git_commits.commit_hash = sonar_analysis.revision
            INNER JOIN 
                sonar_issues ON sonar_analysis.analysis_key = sonar_issues.creation_analysis_key
            WHERE
                git_commits.merge = 'False' 
                AND sonar_issues.type = 'CODE_SMELL' 
            GROUP BY git_commits.project_id
        """)

        for result in self.data_set.fetchall():
            print("Updating amout sonar smells to: ", result)
            self.local_db.execute(
                """
                    UPDATE 
                        project_information 
                    SET 
                        amount_sonar_smells = ?
                    WHERE 
                        project_id = ? 
                """,
                (result)
            )
        self.conn_local_db.commit()
            
    # Lê o Data Set e grava no banco local a quantidade de code smells para cada dev
    def read_amout_code_smells_author(self):
        self.data_set.execute("""
            SELECT
                COUNT(DISTINCT sonar_analysis.revision) AS amount_code_smells,
                git_commits.project_id,
                git_commits.author
            FROM git_commits
            INNER JOIN
                git_commits_changes ON git_commits.commit_hash = git_commits_changes.commit_hash
            INNER JOIN 
                sonar_analysis ON git_commits.commit_hash = sonar_analysis.revision
            INNER JOIN 
                sonar_issues ON sonar_analysis.analysis_key = sonar_issues.creation_analysis_key
            WHERE
                git_commits.merge = 'False' 
                AND sonar_issues.rule LIKE 'code_smells:%' 
            GROUP BY git_commits.author
        """)

        for result in self.data_set.fetchall():
            print("Updating amout code smells to: ", result)
            self.local_db.execute(
                """
                    UPDATE 
                        author_information 
                    SET 
                        amount_code_smells = ?
                    WHERE 
                        project_id = ? 
                        AND author = ?
                """,
                (result)
            )
        self.conn_local_db.commit()

    # Lê o Data Set e grava no banco local a quantidade de code smells para cada projeto
    def read_amout_code_smells_project(self):
        self.data_set.execute("""
            SELECT
                COUNT(DISTINCT sonar_analysis.revision) AS amount_code_smells,
                git_commits.project_id
            FROM git_commits
            INNER JOIN
                git_commits_changes ON git_commits.commit_hash = git_commits_changes.commit_hash
            INNER JOIN 
                sonar_analysis ON git_commits.commit_hash = sonar_analysis.revision
            INNER JOIN 
                sonar_issues ON sonar_analysis.analysis_key = sonar_issues.creation_analysis_key
            WHERE
                git_commits.merge = 'False' 
                AND sonar_issues.rule LIKE 'code_smells:%' 
            GROUP BY git_commits.project_id
        """)

        for result in self.data_set.fetchall():
            print("Updating amout code smells to: ", result)
            self.local_db.execute(
                """
                    UPDATE 
                        project_information 
                    SET 
                        amount_code_smells = ?
                    WHERE 
                        project_id = ? 
                """,
                (result)
            )
        self.conn_local_db.commit()

    # Lê o Data Set e grava no banco local a quantidade de linhas editadas para cada dev
    def read_number_lines_edited_author(self):
        self.data_set.execute("""
            SELECT
                (lines_added + lines_removed) as number_lines_edited,
                git_commits.project_id,
                git_commits.author
            FROM git_commits
            INNER JOIN
                git_commits_changes ON git_commits.commit_hash = git_commits_changes.commit_hash
            WHERE
                git_commits.merge = 'False' 
            GROUP BY git_commits.author
        """)

        for result in self.data_set.fetchall():
            print("Updating number lines edited to: ", result)
            self.local_db.execute(
                """
                    UPDATE 
                        author_information 
                    SET 
                        number_lines_edited = ?
                    WHERE 
                        project_id = ? 
                        AND author = ?
                """,
                (result)
            )
        self.conn_local_db.commit()

    # Lê o Data Set e grava no banco local a quantidade de linhas editadas para cada projeto
    def read_number_lines_edited_project(self):
        self.data_set.execute("""
            SELECT
                (lines_added + lines_removed) as number_lines_edited,
                git_commits.project_id
            FROM git_commits
            INNER JOIN
                git_commits_changes ON git_commits.commit_hash = git_commits_changes.commit_hash
            WHERE
                git_commits.merge = 'False' 
            GROUP BY git_commits.project_id
        """)

        for result in self.data_set.fetchall():
            print("Updating number lines edited to: ", result)
            self.local_db.execute(
                """
                    UPDATE 
                        project_information 
                    SET 
                        number_lines_edited = ?
                    WHERE 
                        project_id = ? 
                """,
                (result)
            )
        self.conn_local_db.commit()
    
    # Pega o primeiro commit e o último de cada author em cada projeto e calcula algumas informações
    def calculate_author_infos(self):
        self.data_set.execute("""
            SELECT DISTINCT
                project_id,
                author,
                MIN(author_date) as first_commit,
                MAX(author_date) as last_commit,
                COUNT(DISTINCT commit_hash) as amount_commits
            FROM 
                git_commits
            WHERE 
                merge='False'
            GROUP BY project_id, author
        """)

        for result in self.data_set.fetchall():
            project_id = result[0]
            author = result[1]
            first_commit = result[2].replace('Z', '+00:00').replace('T', ' ')
            last_commit = result[3].replace('Z', '+00:00').replace('T', ' ')
            amount_commits = result[4]
            single_commit = 0
            
            date_format = '%Y-%m-%d %H:%M:%S%z'
            first_date = datetime.strptime(first_commit, date_format)
            last_date = datetime.strptime(last_commit, date_format)
            delta = last_date - first_date
            days = delta.days
            hours = round((delta.total_seconds() / 3600), 2)

            if(first_date == last_date):
                single_commit = 1
            
            print("Updating project experience to project_id {} author {}".format(project_id, author))
            self.local_db.execute(
                """
                    UPDATE 
                        author_information
                    SET 
                        project_experience_in_days = ?,
                        project_experience_in_hours = ?,
                        single_commit = ?,
                        first_commit = ?,
                        last_commit = ?,
                        amount_commits = ?
                    WHERE 
                        project_id = ?
                        AND author = ?
                """,
                (days, hours, single_commit, first_date, last_date, amount_commits, project_id, author)
            )
        self.conn_local_db.commit()

    # Pega o primeiro commit e o último de cada projeto e calcula a diferença
    def calculate_project_infos(self):
        self.data_set.execute("""
            SELECT DISTINCT
                project_id,
                MIN(author_date) as first_commit,
                MAX(author_date) as last_commit,
                COUNT(DISTINCT commit_hash) as amount_commits
            FROM 
                git_commits
            WHERE 
                merge='False'
            GROUP BY project_id
        """)

        for result in self.data_set.fetchall():
            project_id = result[0]
            first_commit = result[1].replace('Z', '+00:00').replace('T', ' ')
            last_commit = result[2].replace('Z', '+00:00').replace('T', ' ')
            amount_commits = result[3]
            
            date_format = '%Y-%m-%d %H:%M:%S%z'
            first_date = datetime.strptime(first_commit, date_format)
            last_date = datetime.strptime(last_commit, date_format)
            delta = last_date - first_date
            days = delta.days
            hours = round((delta.total_seconds() / 3600), 2)
            
            self.local_db.execute(
                """
                    UPDATE 
                        project_information
                    SET 
                        total_time_in_days = ?,
                        total_time_in_hours = ?,
                        first_commit = ?,
                        last_commit = ?,
                        amount_commits = ?
                    WHERE 
                        project_id = ?
                """,
                (days, hours, first_date, last_date, amount_commits, project_id)
            )
        self.conn_local_db.commit()


if __name__ == "__main__":
    research = Research(fast=True)
    
    # research.read_amout_sonar_smells_author()
    # research.read_amout_sonar_smells_project()
    
    # research.read_amout_code_smells_author()
    # research.read_amout_code_smells_project()

    research.read_number_lines_edited_author()
    research.read_number_lines_edited_project()

    # research.calculate_author_infos()
    # research.calculate_project_infos()
