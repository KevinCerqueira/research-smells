import numpy as np
import scipy.stats
import os
import sqlite3

def cohens_d(group1, group2):
    mean1, mean2 = np.mean(group1), np.mean(group2)
    var1, var2 = np.var(group1, ddof=1), np.var(group2, ddof=1)
    pooled_var = (len(group1) * var1 + len(group2) * var2) / (len(group1) + len(group2))
    d = (mean1 - mean2) / np.sqrt(pooled_var)
    return d

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

path_local_db = os.path.join(BASE_DIR, "research_cv_maior_1.sqlite")
conn_local_db = sqlite3.connect(path_local_db)
local_db = conn_local_db.cursor()

local_db.execute(f"""
    SELECT DISTINCT
        project_id,
        author,
        SUM(CASE WHEN amount_commits < 97 THEN amount_commits ELSE 0 END) AS LOW,
        SUM(CASE WHEN amount_commits >= 97 THEN amount_commits ELSE 0 END) AS HIGH
    FROM
        author_information
    WHERE amount_commits IS NOT NULL AND amount_code_smells IS NOT NULL
    GROUP BY project_id, author;
""")

x = []
y = []

for result in local_db.fetchall():
    column_x = 0
    column_y = 0

    if(result[1] != None):
        column_x = result[2]
    if(result[2] != None):
        column_y = result[3]

    x.append(column_x)
    y.append(column_y)

print(cohens_d(x, y))