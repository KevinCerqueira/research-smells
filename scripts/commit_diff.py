import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Conectando ao SQLite
conn = sqlite3.connect('research_cv.sqlite')

# Consulta SQL para obter a diferença de tempo entre commits para cada autor
query = """
SELECT project_id, author, diff_date
FROM commit_diff
"""

# Lendo a consulta SQL para um DataFrame do pandas
df = pd.read_sql_query(query, conn)

# Convertendo 'diff_date' para o tipo float
df['diff_date'] = df['diff_date'].astype(float)

# Calculando a média e o desvio padrão da diferença de tempo entre commits por autor
stats_df = df.groupby(['project_id','author'])['diff_date'].agg(['mean', 'std'])

# Calculando o Coeficiente de Variação
stats_df['cv'] = stats_df['std'] / stats_df['mean']

# Fechando a conexão com o SQLite
conn.close()

# Ordenando o dataframe pela coluna cv
stats_df = stats_df.sort_values(by='cv')

print(stats_df.to_string())
stats_df.to_csv('cv_results_3.csv')
