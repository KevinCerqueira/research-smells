import pandas as pd
from IPython.display import display, HTML
from scipy import stats
import numpy as np
from cliffs_delta import cliffs_delta as cd
import math

def calcular_tamanho_efeito(u, n1, n2):
    """
    Calcula o tamanho do efeito (r) para o teste U de Mann-Whitney.

    :param u: Valor do teste U de Mann-Whitney.
    :param n1: Tamanho da amostra do grupo 1.
    :param n2: Tamanho da amostra do grupo 2.
    :return: Tamanho do efeito (r).
    """
    numerador = u - (n1 * n2 / 2)
    denominador = (n1 * n2 * ((n1 + n2 + 1) / 12)) ** 0.5
    r = numerador / denominador
    return r

data = pd.read_csv("developersandsmells.csv", index_col=False)

data.boxplot(column='commits', figsize=(10,10), grid=False, fontsize=12)

data.loc[data['commits']< 97, 'dcommits'] = "LOW"
data.loc[data['commits']>= 97, 'dcommits'] = "HIGH"

data.boxplot(column='code_smells', by='dcommits', figsize=(10,10), grid=False, fontsize=12)

data.boxplot(column='code_smells', by='dcommits', showfliers=False, figsize=(10,10), grid=False, fontsize=12)

data['cspercommits'] = data['code_smells'] / data['commits']

data.boxplot(column='cspercommits', by='dcommits', figsize=(10,10), grid=False, fontsize=12)

data.boxplot(column='cspercommits', by='dcommits', showfliers=False, figsize=(10,10), grid=False, fontsize=12)

data[["dcommits", "cspercommits"]].groupby(['dcommits']).describe()

CommitsLow = data[(data['dcommits']=="LOW")]
CommitsHigh = data[(data['dcommits']=="HIGH")]


from scipy import stats
from statsmodels.stats import weightstats as stests
from scipy.stats import mannwhitneyu

### EFFECT SIZE CSPERCOMMIT

utest, pval = mannwhitneyu(CommitsLow['cspercommits'], CommitsHigh['cspercommits'], alternative='two-sided')
print("utest=", float(utest))
print("pval=", float(pval))
if pval < 0.05:
    print("Hipótese nula é falsa")
else:
    print("Hipótese nula é verdadeira")

# Tamanho das amostras
n1 = len(CommitsLow['cspercommits'])
n2 = len(CommitsHigh['cspercommits'])

# Cálculo do tamanho do efeito
r = utest / (n1 * n2)
print("Tamanho do efeito (commits) (r) =", r, n1, n2)

print("\n\n\n\n\ncliff_commit", cd(CommitsLow['cspercommits'], CommitsHigh['cspercommits']))
print("\n qtd commits", len(CommitsLow['cspercommits']), len(CommitsHigh['cspercommits']))