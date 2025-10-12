import pandas as pd
import os

# Caminhos dos arquivos
input_path = ".silver/.silver_aposentados/step06_aposentados.parquet"
output_path = ".silver/.silver_aposentados/step07_aposentados.parquet"

def ajustar_gerais():
    # Carregar o arquivo
    df = pd.read_parquet(input_path)

    # Contar registros originais
    total_original = len(df)

    # Garantir tipos compatíveis
    df["CO_PODER"] = df["CO_PODER"].astype("Int64")
    df["CO_TIPO_PODER"] = df["CO_TIPO_PODER"].astype("Int64")
    df["CO_TIPO_APOSENTADORIA"] = df["CO_TIPO_APOSENTADORIA"].astype("string")
    df["CO_CONDICAO_APOSENTADO"] = df["CO_CONDICAO_APOSENTADO"].astype("string")

    # Ajuste CO_PODER com base no NO_ORGAO
    cond_tcm = df["NO_ORGAO"] == "TRIBUNAL DE CONTAS DO MUNICIPIO DE SAO PAULO"
    cond_camara = df["NO_ORGAO"] == "Câmara Municipal de São Paulo"
    cond_demais = ~(cond_tcm | cond_camara)

    ajustes_poder_tcm = cond_tcm.sum()
    ajustes_poder_camara = cond_camara.sum()
    ajustes_poder_demais = cond_demais.sum()

    df.loc[cond_tcm, "CO_PODER"] = 5
    df.loc[cond_camara, "CO_PODER"] = 2
    df.loc[cond_demais, "CO_PODER"] = 1

    # Ajuste CO_TIPO_PODER com base no NO_ORGAO
    cond_prefeitura = df["NO_ORGAO"] == "PREFEITURA DO MUNICIPIO DE SAO PAULO"
    cond_tipo_1 = cond_prefeitura | cond_camara | cond_tcm
    cond_tipo_2 = ~(cond_tipo_1)

    ajustes_tipo_1 = cond_tipo_1.sum()
    ajustes_tipo_2 = cond_tipo_2.sum()

    df.loc[cond_tipo_1, "CO_TIPO_PODER"] = 1
    df.loc[cond_tipo_2, "CO_TIPO_PODER"] = 2

    # Ajuste CO_TIPO_APOSENTADORIA em branco com base na condição
    cond_vazio = df["CO_TIPO_APOSENTADORIA"].isna() | (df["CO_TIPO_APOSENTADORIA"].str.strip() == "")
    cond_1 = cond_vazio & (df["CO_CONDICAO_APOSENTADO"] == "1")
    cond_2 = cond_vazio & (df["CO_CONDICAO_APOSENTADO"] == "2")

    ajustes_2_cond1 = cond_1.sum()
    ajustes_4_cond2 = cond_2.sum()

    df.loc[cond_1, "CO_TIPO_APOSENTADORIA"] = "2"
    df.loc[cond_2, "CO_TIPO_APOSENTADORIA"] = "4"

    # Salvar o resultado
    df.to_parquet(output_path, index=False)

    # Contar registros ajustados
    total_ajustado = len(df)

    # Imprimir os resultados
    print("Ajustes realizados:")
    print(" - CO_PODER ajustado:")
    print(f"    - TCM (5): {ajustes_poder_tcm}")
    print(f"    - Câmara (2): {ajustes_poder_camara}")
    print(f"    - Demais (1): {ajustes_poder_demais}")
    print(" - CO_TIPO_PODER ajustado:")
    print(f"    - Prefeitura, Câmara e TCM (1): {ajustes_tipo_1}")
    print(f"    - Demais (2): {ajustes_tipo_2}")
    print(" - CO_TIPO_APOSENTADORIA ajustado:")
    print(f"    - Condição 1 → '2': {ajustes_2_cond1}")
    print(f"    - Condição 2 → '4': {ajustes_4_cond2}")
    print(f"\n✅ Total de registros no arquivo original: {total_original}")
    print(f"✅ Total de registros no arquivo ajustado: {total_ajustado}")
    print(f"📁 Arquivo salvo em: {output_path}")

# Execução direta
if __name__ == "__main__":
    ajustar_gerais()