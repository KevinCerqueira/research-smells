import pandas as pd

# Carregar as duas planilhas em DataFrames do pandas
df1 = pd.read_excel('diferenca.xlsx')
df2 = pd.read_excel('total.xlsx')

# Presumindo que 'coluna1' e 'coluna2' são as colunas que formam o ID dos dados
df1.set_index(['project_id', 'author'], inplace=True)
df2.set_index(['project_id', 'author'], inplace=True)

# Selecionar as colunas de interesse da primeira planilha
colunas_selecionadas = df1[['Media de tempo entre commits', 'Desvio Padrão', 'CV']]
# colunas_selecionadas = df1[['mean', 'std', 'cv']]

# Adicionar as colunas selecionadas à segunda planilha, com base no ID
df2 = df2.join(colunas_selecionadas)

# Excluir as linhas com valores ausentes nas colunas adicionadas
# df2.dropna(subset=['Media de tempo entre commits', 'Desvio Padrão', 'CV'], inplace=True)
# df2.dropna(subset=['mean', 'std', 'cv'], inplace=True)





df2.reset_index(inplace=True)

# df_final = pd.merge(df2, df1[['Media de tempo entre commits', 'Desvio Padrão', 'CV']],
#                     on=['project_id', 'author'], how='inner')

# Salvar a nova planilha
df2.to_excel('planilha_final_total_diferenca_2_3.xlsx')
