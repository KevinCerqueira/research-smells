import os
import sqlite3
import platform
import numpy as np
from scipy.stats import shapiro, probplot, spearmanr, mannwhitneyu, pearsonr
# from sklearn.preprocessing import PowerTransformer
import matplotlib.pyplot as plt
# from sklearn.preprocessing import StandardScaler
from datetime import datetime

class Graphs:

    def __init__(self, fast: bool = False):
        # Path do script
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        
        # Conectando ao banco local
        path_local_db = os.path.join(BASE_DIR, "research_cv_maior_5.sqlite")
        self.conn_local_db = sqlite3.connect(path_local_db)
        self.local_db = self.conn_local_db.cursor()
    
    # Acessa as envs    
    def env(self, var):
        env = '\\.env'
        print(platform.system())
        if(platform.system() in ['Linux', 'Darwin']):
            env = '/.env'
            
        with open(os.path.dirname(os.path.realpath(__file__)) + env, 'r', encoding='utf-8') as file_env:
            line = file_env.readline()
            while(line):
                content = line.split('=')
                if(content[0] == var):
                    return content[1]
                line = file_env.readline()

    # Pega 2 colunas da base de dados construida, r = round
    def get_columns(self, x, y, r=False):
        self.local_db.execute(f"""
                SELECT DISTINCT
                    project_id,
                    author,
                    {x},
                    {y}
                FROM
                    cv
                WHERE cv is not null and {x} is not null and {y} is not null 
            """)

        x = []
        y = []
        
        for result in self.local_db.fetchall():
            column_x = 0
            column_y = 0
            
            if(result[1] != None):
                column_x = result[2]
            if(result[2] != None):
                column_y = result[3]
                

            if(r):
                column_x = round(column_x)
                column_y = round(column_y)
    
            x.append(column_x)
            y.append(column_y)
            
        return (np.array(x), np.array(y))
    
    # Spearman
    def spearman(self, x, y, plot=True):
        column_x = x
        column_y = y
        x, y = self.get_columns(x, y)
        # Calcular o coeficiente de correlação de Spearman
        corr_coef, p_value = spearmanr(x, y)
        if(plot):
            # Plotar o gráfico de dispersão
            plt.scatter(x, y)
            plt.title(f'Gráfico de Dispersão: {column_x} VS {column_y} (Spearman)')
            plt.xlabel(column_x)
            plt.ylabel(column_y)
            p_value_formatado = "{:.2e}".format(p_value)
            # Imprimir o coeficiente de correlação de Spearman
            print("Coeficiente de correlação de Spearman:", corr_coef)
            print("p-value", p_value_formatado)
            plt.text(0, -0.25, "Coeficiente de correlação de Spearman: {}".format(corr_coef),
            bbox=dict(facecolor='red', alpha=0.5))
            plt.savefig('../figures/{}_spearman_{}X{}.png'.format(datetime.now().strftime("%Y%m%d%H%M%S"),column_x,column_y))
        
            plt.show()
        return (corr_coef, p_value)

if __name__ == "__main__":
    
    graph = Graphs()
    
    columns_all = ["code_smells"]
    columns = ["lines_edited","commits", "sonar_smells", "cv"]
    
    print("\nmetodo,coeficiente,p_value,coluna_x,coluna_y")
    for x in range(len(columns_all)):
        for y in range(len(columns)):
            coef, p_value = graph.spearman(columns_all[x], columns[y], False)
            print(f"Spearman,{coef},{p_value},{columns_all[x]},{columns[y]}")