import pandas as pd
from IPython.display import display, HTML

data = pd.read_csv("developersandsmells.csv", index_col=False)

display(data)

# # --------------------------------------------------------------------------------
data.boxplot(column='lines_edited', figsize=(10,10), grid=False, fontsize=12)
data[["lines_edited"]].describe()

# # --------------------------------------------------------------------------------
data.boxplot(column='code_smells', figsize=(10,10), grid=False, fontsize=12)
data[["code_smells"]].describe()

# # --------------------------------------------------------------------------------
data.loc[data['commits']< 97, 'dcommits'] = "LOW"
data.loc[data['commits']>= 97, 'dcommits'] = "HIGH"

display(data)
# # --------------------------------------------------------------------------------
data.boxplot(column='code_smells', by='dcommits', figsize=(10,10), grid=False, fontsize=12)
data[["dcommits", "code_smells"]].groupby(['dcommits']).describe()

data.boxplot(column='code_smells', by='dcommits', showfliers=False, figsize=(10,10), grid=False, fontsize=12)
data[["dcommits", "code_smells"]].groupby(['dcommits']).describe()
# # --------------------------------------------------------------------------------

import numpy as np
import scipy.stats
print(data[["dcommits", "code_smells"]])

data['cspercommits'] = data['code_smells'] / data['commits']

data.boxplot(column='cspercommits', by='dcommits', figsize=(10,10), grid=False, fontsize=12)
data[["dcommits", "cspercommits"]].groupby(['dcommits']).describe()

data.boxplot(column='cspercommits', by='dcommits', showfliers=False, figsize=(10,10), grid=False, fontsize=12)
data[["dcommits", "cspercommits"]].groupby(['dcommits']).describe()

CommitsLow = data[(data['dcommits']=="LOW")]
CommitsHigh = data[(data['dcommits']=="HIGH")]




from scipy import stats
from statsmodels.stats import weightstats as stests
from scipy.stats import mannwhitneyu

utest, pval = mannwhitneyu(CommitsLow['cspercommits'], CommitsHigh['cspercommits'], alternative='two-sided')
print("utest=", float(utest))
print("pval=", float(pval))
if pval<0.05:
  print("Hipótese nula é falsa")
else:
  print("Hipótese nula é verdadeira")
print("\n")


# Calcula o tamanho das amostras
n1 = len(CommitsLow['cspercommits'])
n2 = len(CommitsHigh['cspercommits'])

# Calcula o rank-biserial correlation
rank_biserial = 1 - (2 * utest) / (n1 * n2)
print("Rank-biserial correlation:", rank_biserial)