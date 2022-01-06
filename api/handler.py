# Utiliza a biblioteca Flask para trabalhar em ambiente web.
from flask import Flask, request, Response
import pandas as pd
import pickle
from rossmann.Rossmann import Rossmann

# loading trained model in memory
model = pickle.load(open(r'D:\Data_Science\DS em Produção\repos\DataScience_em_Producao\model\model_rossmann.pkl', 'rb'))

# initialize API.
# Cria uma isntância app da classe Flask.
app = Flask(__name__)

# Cria o end point (URL) que vai receber a requisição.
@app.route('/rossmann/predict', methods=['POST'])   # Define a URL /rossmann/predict para receber as requisições. Aceita apenas os métodos POST.
def rossmann_predict():
    
    # Armazena na variável test_json os dados recebidos no formato json via API.
    test_json = request.get_json()
            
    # Verifica se recebeu algum dado.
    if test_json:
        
        # identifica se recebeu um ou mais dados. Se for um dicionário significa que recebeu apenas um dado.
        if isinstance(test_json, dict):
            
            # Converte o dado no formato json em um dataframe e armazena na variável test_raw.
            test_raw = pd.DataFrame(test_json, index=[0])

        else: 
            # Executa o comando abaixo quando recebe vários jsons concatenados.
            # Converte as chaves do json nos nomes das colunas, armazenando as chaves da primeira linha utilizando o comando columns=test_json[0].keys().
            test_raw = pd.DataFrame(test_json, columns=test_json[0].keys())
        
        # Cria a instância pipeline da classe Rossmann
        pipeline = Rossmann()
        
        # data cleaning
        df1 = pipeline.data_cleaning(test_raw)
        
        # feature engineering
        df2 = pipeline.feature_engineering(df1)
        
        # data preparation
        df3 = pipeline.data_preparation(df2)
        
        # prediction
        df_response = pipeline.get_prediction(model, test_raw, df3)
        
        return df_response
       
    else:
        # Caso não recebeu dado, retorna vazio e o status 200, significando que a requisição funcionou, porém a execução não funcionou. mimetype define que é uma aplicação json.
        return Response('{}', status=200, mimetype='application/json')
                                                                    
    
if __name__ == '__main__':
    app.run('0.0.0.0')  # Executa a instância app no host local.