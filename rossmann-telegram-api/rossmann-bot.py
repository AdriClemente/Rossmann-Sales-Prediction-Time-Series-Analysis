import pandas as pd
import json
import requests
from flask import Flask, request, Response
import os

# constants
TOKEN = '1234567890:abcdefghijklmnopqrstuwyxzABCDEFGHIJ'

# info about the Bot
# https://api.telegram.org/bot1234567890:abcdefghijklmnopqrstuwyxzABCDEFGHIJ/getMe

# get updates
# https://api.telegram.org/bot1234567890:abcdefghijklmnopqrstuwyxzABCDEFGHIJ/getUpdates

# setWebhook local
# # https://api.telegram.org/bot1234567890:abcdefghijklmnopqrstuwyxzABCDEFGHIJ/setWebhook?url=https://e080-2804-14d-7830-8c46-887c-34e8-e3b6-d1c5.ngrok.io

# setWebhook Heroku
# # https://api.telegram.org/bot1234567890:abcdefghijklmnopqrstuwyxzABCDEFGHIJ/setWebhook?url=https://rossmann-acel-telegram-bot.herokuapp.com

# send message
# https://api.telegram.org/bot1234567890:abcdefghijklmnopqrstuwyxzABCDEFGHIJ/sendMessage?chat_id=1561073042&text=Hi Adriano, I am ok, tks!

def send_message(chat_id, text):
    url = 'https://api.telegram.org/bot{}/'.format(TOKEN)
    url = url + 'sendMessage?chat_id={}'.format(chat_id)

    r = requests.post(url, json={'text': text})
    print('Status Code {}'.format(r.status_code))

    return None


def load_dataset(store_id):
    # loading test dataset
    df10 = pd.read_csv('test.csv', low_memory=False)
    df_store_raw = pd.read_csv('store.csv', low_memory=False)

    # merge test dataset + store
    df_test = pd.merge(df10, df_store_raw, how='left', on='Store')

    # choose store for prediction
    df_test = df_test[df_test['Store'] == store_id]

    if not df_test.empty:  # verifica se o store_id enviado existe no dataset de treino.
        # remove closed days
        df_test = df_test[df_test['Open'] != 0]
        df_test = df_test[~df_test['Open'].isnull()]  # utiliza o simbolo ~ para selecionar as lojas que nao possuem valor vazio na variavel Open.
        df_test = df_test.drop('Id', axis=1)  # Remove a coluna Id que nao eh utilizada.

        # convert Dataset to json
        # No comando abaixo o parametro orient determina o tipo de valor do dicionario. orient='records' = chave: valor = list like [{column -> value}, … , {column -> value}]
        data = json.dumps(df_test.to_dict(orient='records'))

    else:
        data = 'error'  # Armazena a string 'error' em data caso o store_id não exista no dataset de treino.
 
    return data


def predict(data):
    # API Call
    # define o end point para onde o pedido sera enviado.
    url = 'https://rossmann-model-adriano.herokuapp.com//rossmann/predict'

    # informa para a API qual o tipo de dado que ela esta recebendo.
    header = {'Content-type': 'application/json'}  # define o tipo de requisicao json.

    data = data

    # Envia a requisicao para a API
    # Quando utiliza o metodo POST eh obrigatorio enviar algum dado, por isso foi definido o campo data.
    # Armazena a resposta na variável r.
    r = requests.post(url, data, headers=header)

    print('Status Code {}'.format(r.status_code))

    # Converte a varivel r que esta no formato json e que foi retornada no requisicao da API em um dataframe.
    d1 = pd.DataFrame(r.json(), columns=r.json()[0].keys())

    return d1

# Função para extrair o chat_id e store_id da mensagem recebida.
def parse_message(message):
    chat_id = message['message']['chat']['id']  # armazena na variável chat_id o valor do chat_id do JSON.
    store_id = message['message']['text']  # armazena na variável store_id o valor do campo text do JSON.
    
    store_id = store_id.replace('/', '')  # remove a / utilizada no comando do Telegram.
    
    try:  # verifica se o store_id é um número inteiro
        store_id = int(store_id)

    except ValueError:  # caso o store_id não seja um número inteiro, armazena a strint 'error'.
        store_id = 'error'
        
    return chat_id, store_id
    

# API Initialize
app = Flask(__name__)  # cria uma instância do Flask.

# end point create
@app.route('/', methods=['GET', 'POST'])  # cria um end point na raiz e permite os métodos GET e POST.
def index():  # Executa a função index abaixo toda vez que o end point for acionado recebendo um dado.
    if request.method == 'POST':
        message = request.get_json()  # Armazena na variável message o JSON recebido.

        chat_id, store_id = parse_message(message)

        if store_id != 'error':  # Executa caso o store_id for um número inteiro.
            # loading data
            data = load_dataset(store_id)

            if data != 'error':  # Executa caso o store_id exista no dataset de teste.
                # prediction
                d1 = predict(data)

                # calculation
                # Realiza a soma das predicoes da loja para as proximas 6 semanas.
                d2 = d1[['store', 'prediction']].groupby('store').sum().reset_index()

                # send message
                msg = 'Store Number {} will sell R${:,.2f} in the next 6 weeks'.format(
                           d2['store'].values[0],
                           d2['prediction'].values[0])
                
                send_message(chat_id, msg)
                return Response('Ok', status=200)  # retorna uma mensagem para a API informando que conseguiu enviar
                                                   # a mensagem. Deve enviar o status=200 para que a API não fique
                                                   # rodando indefinidamente.

            else:  # envia uma mensagem caso o store_id não for encontrado no dataset de teste.
                send_message(chat_id, 'Store Not Available')
                return Response('OK', status=200)  # retorna uma mensagem para a API informando que conseguiu enviar
                                                   # a mensagem. Deve enviar o status=200 para que a API não fique
                                                   # rodando indefinidamente.


        else:  # envia uma mensagem caso o store_id não for um número.
            send_message(chat_id, 'Store ID is Wrong')
            return Response('OK', status=200)  # retorna uma mensagem para a API informando que conseguiu enviar
                                               # a mensagem. Deve enviar o status=200 para que a API não fique
                                               # rodando indefinidamente.
        
            

    else:
        # exibe uma mensagem caso o usuário acesse o end point mas não envie nenhum dado.
        return '<h1> Rossmann Telegram BOT <h1>'

if __name__ == '__main__':
    port = os.environ.get('PORT', 5000)
    app.run(host='0.0.0.0', port=port)

