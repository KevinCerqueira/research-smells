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

print("\n\n\n\n\n\cliff_commit", cd(CommitsLow['cspercommits'], CommitsHigh['cspercommits']))
print("\n qtd commits", len(CommitsLow['cspercommits']), len(CommitsHigh['cspercommits']))



## EFFECT SIZE CSPERLINESEDITED

data.boxplot(column='lines_edited', showfliers=False, figsize=(10,10), grid=False, fontsize=12)
data[["lines_edited"]].describe()

data.loc[data['lines_edited']< 18890, 'dlines'] = "LOW"
data.loc[data['lines_edited']>= 18890, 'dlines'] = "HIGH"

data.boxplot(column='code_smells', by='dlines', figsize=(10,10), grid=False, fontsize=12)
data[["dlines", "code_smells"]].groupby(['dlines']).describe()

data.boxplot(column='code_smells', by='dlines', showfliers=False, figsize=(10,10), grid=False, fontsize=12)
data[["dlines", "code_smells"]].groupby(['dlines']).describe()

data['csperlinesedited'] = data['code_smells'] / data['lines_edited']

data.boxplot(column='csperlinesedited', by='dlines', figsize=(10,10), grid=False, fontsize=12)
data[["dlines", "csperlinesedited"]].groupby(['dlines']).describe()

data.boxplot(column='csperlinesedited', by='dlines', showfliers=False, figsize=(10,10), grid=False, fontsize=12)
data[["dlines", "csperlinesedited"]].groupby(['dlines']).describe()

LinesLow = data[(data['dlines']=="LOW")]
LinesHigh = data[(data['dlines']=="HIGH")]


from scipy import stats
from statsmodels.stats import weightstats as stests
from scipy.stats import mannwhitneyu



utest, pval = mannwhitneyu(LinesLow['csperlinesedited'], LinesHigh['csperlinesedited'], alternative='two-sided')
print("utest=", float(utest))
print("pval=", float(pval))
if pval < 0.05:
    print("Hipótese nula é falsa")
else:
    print("Hipótese nula é verdadeira")

def cliffs_delta(lst1, lst2):
    m, n = len(lst1), len(lst2)
    all_pairs = np.array([(x, y) for x in lst1 for y in lst2])
    difference_count = np.sum(all_pairs[:, 0] > all_pairs[:, 1]) - np.sum(all_pairs[:, 0] < all_pairs[:, 1])
    return difference_count / (m * n)
  
print(">>>>>>> cliff", cliffs_delta(LinesLow['csperlinesedited'], LinesHigh['cspercommits']))

# Tamanho das amostras
n1 = len(LinesLow['csperlinesedited'])
n2 = len(LinesHigh['csperlinesedited'])

# Cálculo do tamanho do efeito
r = utest / (n1 * n2)
print("Tamanho do efeito (lines) (r) =", r, n1, n2)

print("12222Tamanho do efeito (lines) (r) =", (utest / math.sqrt(n1+n2)), n1, n2)




""""
O valor de r no contexto do teste U de Mann-Whitney é uma medida do tamanho do efeito, que quantifica a magnitude da diferença entre dois grupos independentes. O valor de r é calculado como:

r = U / (n1 x n2)

onde U é a estatística do teste U de Mann-Whitney, n1e n2​ são os tamanhos das amostras dos dois grupos comparados. Este valor pode variar de -1 a 1. Valores próximos de 0 indicam um efeito pequeno, enquanto valores mais próximos de -1 ou 1 indicam um efeito maior.

Não existe uma tabela de descrição universalmente aceita para interpretar o valor de r no teste U de Mann-Whitney, mas uma orientação comum para interpretar o tamanho do efeito é a seguinte:

*   **Pequeno**: ∣r∣ em torno de 0.1
*   **Médio**: ∣r∣em torno de 0.3
*   **Grande**: ∣r∣ em torno de 0.5 ou mais
"""
# print("\n\n\n\n\n\cliff", cliffs_delta(LinesLow['cspercommits'], LinesHigh['cspercommits']))

print("\n\n\n\n\n\cliff_lines", cd(LinesLow['csperlinesedited'], LinesHigh['csperlinesedited']))
print("\n qtd lines", len(LinesLow['csperlinesedited']), len(LinesHigh['csperlinesedited']))


def calcular_tamanho_efeito(u, n1, n2):
    """
    Calcula o tamanho do efeito (r) para o teste U de Mann-Whitney.

    :param u: Valor do teste U de Mann-Whitney.
    :param n1: Tamanho da amostra do grupo 1.
    :param n2: Tamanho da amostra do grupo 2.
    :return: Tamanho do efeito (r).
    """
    # numerador = u - (n1 * n2 / 2)
    # denominador = (n1 * n2 * ((n1 + n2 + 1) / 12)) ** 0.5
    # r = numerador / denominador
    # return r
    return utest / math.sqrt(n1*n2)