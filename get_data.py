# requests: pedidos que um programa ou sistema faz para outro sistema
import requests
import pandas as pd

def get_data(cep):
    endpoint = f"https://viacep.com.br/ws/{cep}/json/"

    try:
        response = requests.get(endpoint, timeout=10)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro ao obter dados para o CEP {cep}. Status code: {response.status_code}")
            return None
        
    except requests.exceptions.ConnectionError as e:
        print(f"Erro de conexão para o cep {cep}: {e}")
        return None
    except requests.exceptions.Timeout as e:
        print(f"Tempo de conexão esgotado para o cep {cep}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer a requisição para o cep {cep}: {e}")
        return None

user_path = "bronze/users.csv"
df_users = pd.read_csv(user_path, sep = ',').head(300)


cep_list = df_users['cep'].tolist() #tranforma uma colona em uma lista

info_list = []

for cep in cep_list:
    cep = cep.replace('-', '') #remove o traço do cep
    cep_info = get_data(cep)
    # se contem a chave "erro" no dicionário cep_info, significa que o cep não foi encontrado
    if "erro" in cep_info:
        print(f"CEP {cep} não encontrado.")
        continue
    info_list.append(cep_info)

info_list = pd.DataFrame(info_list) 
info_list.to_csv("bronze/cep_info.csv", index=False)