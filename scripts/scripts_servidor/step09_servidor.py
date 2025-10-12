# step09_servidor.py

import pandas as pd
import os

# Caminho do arquivo final
file_path = os.path.join("gold", "servidor_final.xlsx")

# Verificar se o arquivo existe
if not os.path.exists(file_path):
    raise FileNotFoundError("Arquivo servidor_final.xlsx não encontrado na pasta gold.")

# Carregar o arquivo Excel
df = pd.read_excel(file_path, engine="openpyxl")

# Validações
expected_records = 116749
total_registros = len(df)
colunas = df.columns.tolist()
total_colunas = len(colunas)
registros_duplicados = df.duplicated().sum()
células_nulas = df.isnull().sum().sum()

# Relatório de validação
print("\n🔍 Validação do arquivo servidor_final.xlsx")
print("----------------------------------------")
print(f"✅ Total de registros: {total_registros} (esperado: {expected_records})")
print(f"📊 Total de colunas: {total_colunas}")
print(f"🗂️ Nomes das colunas: {colunas}")
print(f"⚠️ Registros duplicados: {registros_duplicados}")
print(f"❗ Células nulas: {células_nulas}")

# Consistência geral (verificar se todas as colunas esperadas estão presentes)
esperadas = [
    'ID_SERVIDOR', 'NOME_SERVIDOR', 'CPF', 'DT_NASC_SERVIDOR', 'DT_ING_ENTE',
    'CARGO', 'ESCOLARIDADE', 'TIPO_VINCULO', 'REMUNERACAO', 'SITUACAO'
]
colunas_faltando = [col for col in esperadas if col not in colunas]

if colunas_faltando:
    print(f"🚨 Colunas esperadas ausentes: {colunas_faltando}")
else:
    print("✅ Todas as colunas esperadas estão presentes.")