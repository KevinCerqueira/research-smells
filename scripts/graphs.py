import os
import sqlite3
import platform
import numpy as np
from scipy.stats import shapiro, probplot, spearmanr, mannwhitneyu
from sklearn.preprocessing import PowerTransformer
import matplotlib.pyplot as plt

class Graphs:

    def __init__(self, fast: bool = False):
        # Path do script
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        
        # Conectando ao banco local
        path_local_db = os.path.join(BASE_DIR, self.env("RESEARCH_DB"))
        self.conn_local_db = sqlite3.connect(path_local_db)
        self.local_db = self.conn_local_db.cursor()
    
    # Acessa as envs    
    def env(self, var):
        env = '\\.env'
        if(platform.system() == 'Linux'):
            env = '/.env'
            
        with open(os.path.dirname(os.path.realpath(__file__)) + env, 'r', encoding='utf-8') as file_env:
            line = file_env.readline()
            while(line):
                content = line.split('=')
                if(content[0] == var):
                    return content[1]
                line = file_env.readline()

    # Pega uma coluna da base de dados construida
    def get_one_column(self, column):
        self.local_db.execute(f"""
            SELECT DISTINCT
                project_id,
                author,
                {column}
            FROM
                author_percentage_information
        """)
            
        array = []
        for result in self.local_db.fetchall():
            if result[2] == None:
                array.append(0)
            else:
                array.append(result[2])

        return np.array(array)

    # Pega 2 colunas da base de dados construida, r = round
    def get_columns(self, x, y, r=False):
        self.local_db.execute(f"""
                SELECT DISTINCT
                    project_id,
                    author,
                    {x},
                    {y}
                FROM
                    author_percentage_information
            """)
        
        x = []
        y = []
        for result in self.local_db.fetchall():
            column_x = 0
            column_y = 0
            
            if(result[2] != None):
                column_x = result[2]
            if(result[3] != None):
                column_y = result[3]

            if(r):
                column_x = round(column_x)
                column_y = round(column_y)
    
            x.append(column_x)
            y.append(column_y)
            
        return (np.array(x), np.array(y))
    
    # Shapiro-Wilk somente texto
    def shapiro_text(self, column):
        array = self.get_one_column(column)
        
        stat, p = shapiro(array)
        
        print("SHAPIRO: {}".format(p))
        
        if p > 0.05:
            print(f"{column} são normalmente distribuídas.\n")
        else:
            print(f"{column} não são normalmente distribuídas.\n\n")

        transformer = PowerTransformer(method='yeo-johnson')
        array_normal = transformer.fit_transform(array.reshape(-1, 1))

        stat, p = shapiro(array_normal)

        print("Power Transformer SHAPIRO: {}".format(p))
        if p > 0.05:
            print(f"{column} normalizadas são normalmente distribuídas.\n")
        else:
            print(f"{column} normalizadas não são normalmente distribuídas.")
    
    # Shapiro-Wilk   
    def shapiro_plot(self, column):
        data = self.get_one_column(column)
        # Realiza o teste Shapiro-Wilk
        stat, p = shapiro(data)

        # Plota o gráfico de probabilidade normal
        fig = plt.figure(figsize=(8, 6))
        ax = fig.add_subplot(111)
        res = probplot(data, plot=ax)
        ax.set_title(f'Gráfico de probabilidade normal: {column} (Shapiro-Wilk)')
        ax.set_xlabel(f'{x} (Quantis teóricos)')
        ax.set_ylabel(f'{y} (Quantis observados)')
        plt.show()

        # Imprime o resultado do teste Shapiro-Wilk
        print('Estatística de teste:', stat)
        print('Valor p:', p)
    
    # Mann-Whitney U
    def mannwhitneyu(self, x, y):
        column_x = x
        column_y = y
        x, y = self.get_columns(x, y)

        # Calcula a estatística de teste de Mann-Whitney U
        stat, p = mannwhitneyu(x, y)

        # Cria um array de valores para a distribuição Mann-Whitney
        xmin, xmax = np.min(x), np.max(x)
        ymin, ymax = np.min(y), np.max(y)
        x_values = np.linspace(xmin, xmax, 100)
        y_values = np.linspace(ymin, ymax, 100)
        X, Y = np.meshgrid(x_values, y_values)
        Z = np.zeros_like(X)

        # Verifica se há pelo menos um elemento em x e y antes de gerar a distribuição
        if len(x) > 0 and len(y) > 0:
            for i in range(len(x_values)):
                for j in range(len(y_values)):
                    Z[i,j] = mannwhitneyu(x[x<X[i,j]][0], y[y<Y[i,j]][0])[1]
        else:
            print("Os dados de entrada têm tamanho zero!")

        # Plota o gráfico da distribuição Mann-Whitney
        fig = plt.figure(figsize=(8, 6))
        ax = fig.add_subplot(111, projection='3d')
        ax.plot_surface(X, Y, Z, cmap='viridis')
        ax.set_xlabel(column_x)
        ax.set_ylabel(column_y)
        ax.set_zlabel('p-value')
        plt.show()

    # Pearson
    def pearson(self, x, y):
        column_x = x
        column_y = y
        
        x, y = self.get_columns(x, y)
        # Calcular o coeficiente de correlação de Pearson
        corr_coef = np.corrcoef(x, y)[0,1]

        # Plotar o gráfico de dispersão
        plt.scatter(x, y)
        plt.title(f'Gráfico de Dispersão: {column_x} VS {column_y} (Pearson)')
        plt.xlabel(column_x)
        plt.ylabel(column_y)

        # Imprimir o coeficiente de correlação de Pearson
        print("Coeficiente de correlação de Pearson:", corr_coef)

        plt.show()

    # Spearman
    def spearman(self, x, y):
        column_x = x
        column_y = y
        x, y = self.get_columns(x, y)
        
        # Calcular o coeficiente de correlação de Spearman
        corr_coef, p_value = spearmanr(x, y)

        # Plotar o gráfico de dispersão
        plt.scatter(x, y)
        plt.title(f'Gráfico de Dispersão: {column_x} VS {column_y} (Spearman)')
        plt.xlabel(column_x)
        plt.ylabel(column_y)

        # Imprimir o coeficiente de correlação de Spearman
        print("Coeficiente de correlação de Spearman:", corr_coef)

        plt.show()

if __name__ == "__main__":
    
    graph = Graphs()
    
    while True:
        choose = int(input("""
            - Qual função você deseja acessar?
            1. Shapiro-Wilk (somente texto)
            2. Shapiro-Wilk
            3. Mann-Whitney U
            4. Pearson
            5. Spearman
        """))
        
        if choose > 5 or choose < 1:
            print("Por favor, escolha uma opção válida.")
        else:
            if choose < 3:
                column = str(input("\n\n >> Digite a coluna que deseja aplicar o método: "))
                if choose == 1:
                    graph.shapiro_text(column)
                elif choose == 2:
                    graph.shapiro_plot(column)
            elif choose > 2:
                x = str(input("\n\n >> Digite a coluna que será o X: "))
                y = str(input("\n >> Digite a coluna que será o Y: "))
                if choose == 3:
                    graph.mannwhitneyu(x, y)
                elif choose == 4:
                    graph.pearson(x, y)
                elif choose == 5:
                    graph.spearman(x, y)
        
        ex = str(input("\n\n - Deseja realizar outra operação? (S/n):"))
        if(ex == 'n' or ex == 'N'):
            exit()
                
                
            
    # graph.shapiro_plot("code_smells")
    # graph.mannwhitneyu("lines_edited", "code_smells")
    # graph.pearson("lines_edited", "code_smells")
    # graph.spearman("lines_edited", "code_smells")