import pandas as pd
import os

# Caminhos de entrada e saída
CAMINHO_ENTRADA = os.path.join(".silver", ".silver_pensionistas", "step04_pensionistas.parquet")
CAMINHO_SAIDA = os.path.join(".silver", ".silver_pensionistas", "step05_pensionistas.parquet")

# Verificar se o arquivo existe
if not os.path.exists(CAMINHO_ENTRADA):
    raise FileNotFoundError("Arquivo de entrada não encontrado.")

# Carregar os dados
df = pd.read_parquet(CAMINHO_ENTRADA)

# Contagem inicial
total_entrada = len(df)

# Calcular VL_BENEF_PENSAO ajustado
vl_benef_original = df['VL_BENEF_PENSAO']
df['VL_BENEF_PENSAO_AJUSTADO'] = (df['VL_TOT_PENSAO'] * df['VL_PCT_QUOTA'] / 100).round(2)

# Verificar registros ajustados
mask_ajuste = df['VL_BENEF_PENSAO_AJUSTADO'] != vl_benef_original
ajustes_realizados = mask_ajuste.sum()

# Exibir os 10 primeiros registros ajustados
df_verificacao = df.loc[mask_ajuste, [
    'ID_INSTITUIDOR_MATRICULA',
    'ID_PENSIONISTA_MATRICULA',
    'VL_PCT_QUOTA',
    'VL_TOT_PENSAO',
    'VL_BENEF_PENSAO',
    'VL_BENEF_PENSAO_AJUSTADO'
]]

print("🔍 Registros ajustados (até 10 linhas):")
print(df_verificacao.head(10))

# Substituir VL_BENEF_PENSAO pelos valores ajustados e salvar
df['VL_BENEF_PENSAO'] = df['VL_BENEF_PENSAO_AJUSTADO']
df.drop(columns=['VL_BENEF_PENSAO_AJUSTADO'], inplace=True)
df.to_parquet(CAMINHO_SAIDA, index=False)

# Contagem final
total_saida = len(df)

# Exibir resumo
print(f"\nTotal de registros de entrada: {total_entrada}")
print(f"Total de registros de saída: {total_saida}")
print(f"Total de instituidores únicos: {df['ID_INSTITUIDOR_MATRICULA'].nunique()}")
print(f"Registros ajustados em VL_BENEF_PENSAO: {ajustes_realizados}")
print(f"Arquivo salvo em: {CAMINHO_SAIDA}")
