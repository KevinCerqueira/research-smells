import os
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
                CREATE TABLE IF NOT EXISTS "data" (
                "project_id" TEXT,
                "author"	 TEXT,
                "project_experience_in_days"  REAL,
                "project_experience_in_hours" REAL,
                "number_lines_edited"	INTEGER,
                "amount_commits"		INTEGER,
                "amount_code_smells"	INTEGER,
                "amount_sonar_smells"	INTEGER
                );
            """)
        self.insert_authors_and_projects()

    # Insere todos os projetos e autores no banco de dados local
    def insert_authors_and_projects(self):
        self.data_set.execute("SELECT project_id, author FROM git_commits")
        for result in self.data_set.fetchall():
            self.local_db.execute(
                "SELECT 1 FROM data WHERE project_id = ? AND author = ?", result)
            if (len(self.local_db.fetchall()) == 0):
                print("Insert into database", result)
                self.local_db.execute(
                    "INSERT INTO data (project_id, author) VALUES (?,?)", result)

        self.conn_local_db.commit()

    # Lê o Data Set e grava no banco local a quantidade de sonar smells para cada dev
    def read_amout_sonar_smells(self):
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
                        data 
                    SET 
                        amount_sonar_smells = ?
                    WHERE 
                        project_id = ? 
                        AND author = ?
                """,
                (result)
            )
            self.conn_local_db.commit()
            
    # Lê o Data Set e grava no banco local a quantidade de code smells para cada dev
    def read_amout_code_smells(self):
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
                        data 
                    SET 
                        amount_code_smells = ?
                    WHERE 
                        project_id = ? 
                        AND author = ?
                """,
                (result)
            )
            self.conn_local_db.commit()

    # Lê o Data Set e grava no banco local a quantidade de linhas editadas para cada dev
    def read_number_lines_edited(self):
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
                        data 
                    SET 
                        number_lines_edited = ?
                    WHERE 
                        project_id = ? 
                        AND author = ?
                """,
                (result)
            )
            self.conn_local_db.commit()


if __name__ == "__main__":
    research = Research(fast=True)
    research.read_amout_code_smells()
