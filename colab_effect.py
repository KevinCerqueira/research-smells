import pandas as pd
from scipy import stats

# Carregue seus dados aqui
data = pd.read_csv("developersandsmells.csv", index_col=False)

# Geração de boxplots
data.boxplot(column='commits', figsize=(10,10), grid=False, fontsize=12)

# Classificação dos dados em grupos 'LOW' e 'HIGH'
data.loc[data['commits'] < 97, 'dcommits'] = "LOW"
data.loc[data['commits'] >= 97, 'dcommits'] = "HIGH"

# Mais boxplots
data.boxplot(column='code_smells', by='dcommits', figsize=(10,10), grid=False, fontsize=12)
data.boxplot(column='code_smells', by='dcommits', showfliers=False, figsize=(10,10), grid=False, fontsize=12)

# Cálculo de code smells por commit
data['cspercommits'] = data['code_smells'] / data['commits']

# Boxplots para 'cspercommits'
data.boxplot(column='cspercommits', by='dcommits', figsize=(10,10), grid=False, fontsize=12)
data.boxplot(column='cspercommits', by='dcommits', showfliers=False, figsize=(10,10), grid=False, fontsize=12)

# Agrupamento e descrição dos dados
grouped_data = data[["dcommits", "cspercommits"]].groupby(['dcommits']).describe()

# Separação dos dados em dois grupos para o teste
CommitsLow = data[data['dcommits'] == "LOW"]['cspercommits']
CommitsHigh = data[data['dcommits'] == "HIGH"]['cspercommits']

# Cálculo do effect size usando o teste de Mann-Whitney U
utest, pval = stats.mannwhitneyu(CommitsLow, CommitsHigh, alternative='two-sided')

print(utest, pval)
print("\n\n\n\n")
# Exibindo os resultados
print("U-statistic:", utest)
print("P-value:", pval)
if pval < 0.05:
    print("Hipótese nula rejeitada: Há diferença significativa entre os grupos.")
else:
    print("Hipótese nula aceita: Não há diferença significativa entre os grupos.")
