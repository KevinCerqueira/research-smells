import os
import sqlite3
import platform
import numpy as np
from scipy.stats import shapiro, probplot, spearmanr, mannwhitneyu
from sklearn.preprocessing import PowerTransformer
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler

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
            print(f"{column} são normalmente distribuídas.\n\n")
        else:
            print(f"{column} não são normalmente distribuídas.\n\n")

        transformer = PowerTransformer(method='yeo-johnson')
        array_normal = transformer.fit_transform(array.reshape(-1, 1))

        # scaler = StandardScaler()
        # array_normal = scaler.fit_transform(array.reshape(-1, 1))

        stat, p = shapiro(array_normal)

        print("Power Transformer SHAPIRO: {}".format(p))
        if p > 0.05:
            print(f"{column} normalizadas são normalmente distribuídas.\n\n")
        else:
            print(f"{column} normalizadas não são normalmente distribuídas.\n\n")
    
    # Shapiro-Wilk   
    def shapiro_plot(self, column):
        data = self.get_one_column(column)
        # Realiza o teste Shapiro-Wilk
        stat, p = shapiro(data)

        # Plota o gráfico de probabilidade normal
        fig = plt.figure(figsize=(8, 6))
        ax = fig.add_subplot(111)
        probplot(data, plot=ax)
        ax.set_title(f'Gráfico de probabilidade normal: {column} (Shapiro-Wilk)')
        ax.set_xlabel('Quantis teóricos')
        ax.set_ylabel('Quantis observados')

        # Imprime o resultado do teste Shapiro-Wilk
        print('Estatística de teste:', stat)
        print('Valor p:', p)
        plt.text(0, -0.25, "Estatística do teste: {}\nValor-p: {}".format(stat, p),
                bbox=dict(facecolor='red', alpha=0.5))
        plt.show()
    
    # Mann-Whitney U
    def mannwhitneyu(self, x, y):
        column_x = x
        column_y = y
        x, y = self.get_columns(x, y)

        # Realizar o teste de Mann-Whitney U
        stat, p = mannwhitneyu(x, y)

        # Plotar os pontos das duas amostras em um gráfico de dispersão
        plt.scatter(x, [0] * len(x), alpha=0.5, label=column_x)
        plt.scatter(y, [1] * len(y), alpha=0.5, label=column_y)
        plt.legend(loc="upper right")
        plt.title(f"Gráfico de dispersão: {column_x} VS {column_y} (Mann-Whitney U)")

        # Imprimir o resultado do teste no gráfico
        plt.text(0, -0.25, "Estatística do teste: {}\nValor-p: {}".format(stat, p),
                bbox=dict(facecolor='red', alpha=0.5))

        plt.show()

    # Mann-Whitney U
    def mannwhitneyu_histogram(self, x, y):
        column_x = x
        column_y = y
        x, y = self.get_columns(x, y)


        # Realizar o teste de Mann-Whitney U
        stat, p = mannwhitneyu(x, y)

        # Plotar as duas amostras em um histograma
        plt.hist(x, alpha=0.5, label=column_x)
        plt.hist(y, alpha=0.5, label=column_y)
        plt.legend(loc="upper right")
        plt.title(f"Histograma das amostras {column_x} VS {column_y} (Mann-Whitney U)")

        # Imprimir o resultado do teste no gráfico
        plt.text(0.5, 20, "Estatística do teste: {}\nValor-p: {}".format(stat, p),
                bbox=dict(facecolor='red', alpha=0.5))

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
        plt.text(0, -0.25, "Coeficiente de correlação de Pearson: {}".format(corr_coef),
                bbox=dict(facecolor='red', alpha=0.5))

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
        plt.text(0, -0.25, "Coeficiente de correlação de Spearman: {}".format(corr_coef),
        bbox=dict(facecolor='red', alpha=0.5))
        
    # Gráfico de dispersão
    def scatter(self, x, y):
        column_x = x
        column_y = y
        x, y = self.get_columns(x, y)

        # Plotar o gráfico de dispersão
        plt.scatter(x, y)
        plt.title(f'Gráfico de Dispersão: {column_x} VS {column_y}')
        plt.xlabel(column_x)
        plt.ylabel(column_y)

        plt.show()

if __name__ == "__main__":
    
    graph = Graphs()
    
    columns = ["lines_edited","rounded_lines_edited","commits","rounded_commits","experience_in_days","rounded_experience_in_days","experience_in_hours","rounded_experience_in_hours","code_smells","rounded_code_smells","sonar_smells","rounded_sonar_smells"]
    
    while True:
        choose = int(input("""
            - Qual função você deseja acessar?
            1. Shapiro-Wilk (somente texto)
            2. Shapiro-Wilk
            3. Mann-Whitney U
            4. Mann-Whitney U (histograma)
            5. Pearson
            6. Spearman
            7. Dispersão comum
            
        >> """))
        
        if choose > 7 or choose < 1:
            print("\n\nPor favor, escolha uma opção válida.")
        else:
            if choose < 3:
                column = str(input("\n\n >> Digite a coluna que deseja aplicar o método: "))
                if column not in columns:
                    print("\n Essa coluna não exite!")
                elif choose == 1:
                    graph.shapiro_text(column)
                elif choose == 2:
                    graph.shapiro_plot(column)
            elif choose > 2:
                x = str(input("\n>> Digite a coluna que será o X: "))
                y = str(input(">> Digite a coluna que será o Y: "))
                if(x not in columns or y not in columns):
                    print("\n Coluna inexistente!")
                elif choose == 3:
                    graph.mannwhitneyu(x, y)
                elif choose == 4:
                    graph.mannwhitneyu_histogram(x, y)
                elif choose == 5:
                    graph.pearson(x, y)
                elif choose == 6:
                    graph.spearman(x, y)
                elif choose == 7:
                    graph.scatter(x, y)
        
        ex = str(input(" - Deseja realizar outra operação? (S/n):"))
        if(ex == 'n' or ex == 'N'):
            exit()
                
                
            
    # graph.shapiro_plot("code_smells")
    # graph.mannwhitneyu("lines_edited", "code_smells")
    # graph.pearson("lines_edited", "code_smells")
    # graph.spearman("lines_edited", "code_smells")