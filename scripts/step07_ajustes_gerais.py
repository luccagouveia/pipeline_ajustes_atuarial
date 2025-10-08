# step07_ajustes_gerais.py

import pandas as pd

# Constantes
MAX_MESES_RGPS = 22280

# Caminhos dos arquivos
input_path = ".silver/servidores_ajuste_valores.parquet"
output_path = ".silver/servidores_ajuste_gerais.parquet"

# Carregar o arquivo
df = pd.read_parquet(input_path)

# Garantir tipos compatíveis
df["CO_CRITERIO_ELEGIBILIDADE"] = df["CO_CRITERIO_ELEGIBILIDADE"].astype("Int64")
df["CO_PODER"] = df["CO_PODER"].astype("Int64")
df["CO_TIPO_PODER"] = df["CO_TIPO_PODER"].astype("Int64")

# Regra 1: Preencher CO_CRITERIO_ELEGIBILIDADE com '1' se estiver vazio
ajustes_criterio = df["CO_CRITERIO_ELEGIBILIDADE"].isna().sum()
df["CO_CRITERIO_ELEGIBILIDADE"] = df["CO_CRITERIO_ELEGIBILIDADE"].fillna(1)

# Regra 2: Ajustar CO_PODER com base no NO_ORGAO
cond_tcm = df["NO_ORGAO"] == "TRIBUNAL DE CONTAS DO MUNICIPIO DE SAO PAULO"
cond_camara = df["NO_ORGAO"] == "Câmara Municipal de São Paulo"
cond_demais = ~(cond_tcm | cond_camara)

ajustes_poder_tcm = cond_tcm.sum()
ajustes_poder_camara = cond_camara.sum()
ajustes_poder_demais = cond_demais.sum()

df.loc[cond_tcm, "CO_PODER"] = 5
df.loc[cond_camara, "CO_PODER"] = 2
df.loc[cond_demais, "CO_PODER"] = 1

# Regra 3: Ajustar CO_TIPO_PODER
cond_prefeitura = df["NO_ORGAO"] == "PREFEITURA DO MUNICIPIO DE SAO PAULO"
cond_camara_tipo = df["NO_ORGAO"] == "Câmara Municipal de São Paulo"
cond_tipo_1 = cond_prefeitura | cond_camara_tipo
cond_tipo_2 = ~(cond_tipo_1)

ajustes_tipo_1 = cond_tipo_1.sum()
ajustes_tipo_2 = cond_tipo_2.sum()

df.loc[cond_tipo_1, "CO_TIPO_PODER"] = 1
df.loc[cond_tipo_2, "CO_TIPO_PODER"] = 2

# Regra 4: Limitar NU_TEMPO_RGPS a no máximo 22280 meses
ajustes_rgps = (df["NU_TEMPO_RGPS"] > MAX_MESES_RGPS).sum()
df.loc[df["NU_TEMPO_RGPS"] > MAX_MESES_RGPS, "NU_TEMPO_RGPS"] = MAX_MESES_RGPS

# Salvar o resultado
df.to_parquet(output_path, index=False)

# Imprimir os resultados
print("Ajustes realizados:")
print(f" - CO_CRITERIO_ELEGIBILIDADE preenchido com '1': {ajustes_criterio}")
print(" - CO_PODER ajustado:")
print(f"    - TCM (5): {ajustes_poder_tcm}")
print(f"    - Câmara (2): {ajustes_poder_camara}")
print(f"    - Demais (1): {ajustes_poder_demais}")
print(" - CO_TIPO_PODER ajustado:")
print(f"    - Prefeitura e Câmara (1): {ajustes_tipo_1}")
print(f"    - Demais (2): {ajustes_tipo_2}")
print(f" - NU_TEMPO_RGPS limitado a {MAX_MESES_RGPS}: {ajustes_rgps}")