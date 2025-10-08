import pandas as pd
from datetime import datetime
import os

# Caminhos de entrada e saída
CAMINHO_ENTRADA = os.path.join(".silver\.silver_servidor", "servidor_step02.parquet")
CAMINHO_SAIDA = os.path.join(".silver\.silver_servidor", "servidor_step03.parquet")

def ajustar_datas():
    # Carregar o arquivo Parquet da camada .silver
    df = pd.read_parquet(CAMINHO_ENTRADA)

    # Garantir que os campos estejam no formato datetime
    campos_data = ["DT_ING_SERV_PUB", "DT_ING_CARREIRA", "DT_ING_CARGO", "DT_NASC_SERVIDOR", "DT_ING_ENTE"]
    for campo in campos_data:
        df[campo] = pd.to_datetime(df[campo], errors='coerce', dayfirst=True)

    # Armazenar cópias originais para comparação
    df["ORIG_DT_ING_SERV_PUB"] = df["DT_ING_SERV_PUB"]
    df["ORIG_DT_ING_CARREIRA"] = df["DT_ING_CARREIRA"]
    df["ORIG_DT_ING_CARGO"] = df["DT_ING_CARGO"]

    # Calcular idades com validação de datas
    df["IDADE_SERVIDOR_SP"] = df.apply(
        lambda row: ((row["DT_ING_SERV_PUB"] - row["DT_NASC_SERVIDOR"]).days // 365)
        if pd.notnull(row["DT_ING_SERV_PUB"]) and pd.notnull(row["DT_NASC_SERVIDOR"]) and row["DT_ING_SERV_PUB"] > row["DT_NASC_SERVIDOR"]
        else None,
        axis=1
    )
    df["IDADE_SERVIDOR_CR"] = df.apply(
        lambda row: ((row["DT_ING_CARREIRA"] - row["DT_NASC_SERVIDOR"]).days // 365)
        if pd.notnull(row["DT_ING_CARREIRA"]) and pd.notnull(row["DT_NASC_SERVIDOR"]) and row["DT_ING_CARREIRA"] > row["DT_NASC_SERVIDOR"]
        else None,
        axis=1
    )
    df["IDADE_SERVIDOR_CG"] = df.apply(
        lambda row: ((row["DT_ING_CARGO"] - row["DT_NASC_SERVIDOR"]).days // 365)
        if pd.notnull(row["DT_ING_CARGO"]) and pd.notnull(row["DT_NASC_SERVIDOR"]) and row["DT_ING_CARGO"] > row["DT_NASC_SERVIDOR"]
        else None,
        axis=1
    )

    # Aplicar ajustes por idade < 18
    df.loc[df["IDADE_SERVIDOR_SP"] < 18, "DT_ING_SERV_PUB"] = df["DT_ING_ENTE"]
    df.loc[df["IDADE_SERVIDOR_CR"] < 18, "DT_ING_CARREIRA"] = df["DT_ING_ENTE"]
    df.loc[df["IDADE_SERVIDOR_CG"] < 18, "DT_ING_CARGO"] = df["DT_ING_ENTE"]

    # Regra adicional: DT_ING_SERV_PUB nunca pode ser maior que DT_ING_ENTE
    df.loc[df["DT_ING_SERV_PUB"] > df["DT_ING_ENTE"], "DT_ING_SERV_PUB"] = df["DT_ING_ENTE"]

    # Filtrar registros que sofreram alteração
    alterados = df[
        (df["ORIG_DT_ING_SERV_PUB"] != df["DT_ING_SERV_PUB"]) |
        (df["ORIG_DT_ING_CARREIRA"] != df["DT_ING_CARREIRA"]) |
        (df["ORIG_DT_ING_CARGO"] != df["DT_ING_CARGO"])
    ][[
        "ID_SERVIDOR_MATRICULA",
        "DT_ING_ENTE",
        "ORIG_DT_ING_SERV_PUB", "DT_ING_SERV_PUB",
        "ORIG_DT_ING_CARREIRA", "DT_ING_CARREIRA",
        "ORIG_DT_ING_CARGO", "DT_ING_CARGO"
    ]]

    # Contagem de alterações por campo
    qtd_sp = (df["ORIG_DT_ING_SERV_PUB"] != df["DT_ING_SERV_PUB"]).sum()
    qtd_cr = (df["ORIG_DT_ING_CARREIRA"] != df["DT_ING_CARREIRA"]).sum()
    qtd_cg = (df["ORIG_DT_ING_CARGO"] != df["DT_ING_CARGO"]).sum()

    # Exibir os 10 primeiros registros ajustados no terminal
    print("\nRegistros ajustados (exibindo os 10 primeiros):")
    print(alterados.head(10).to_string(index=False))

    # Exibir contagem de alterações
    print("\nResumo das alterações:")
    print(f"→ DT_ING_SERV_PUB ajustado em {qtd_sp} registros")
    print(f"→ DT_ING_CARREIRA ajustado em {qtd_cr} registros")
    print(f"→ DT_ING_CARGO ajustado em {qtd_cg} registros")

    # Salvar o resultado ajustado na camada .silver
    df.drop(columns=["ORIG_DT_ING_SERV_PUB", "ORIG_DT_ING_CARREIRA", "ORIG_DT_ING_CARGO"], inplace=True)
    df.to_parquet(CAMINHO_SAIDA, index=False)
    print(f"\nArquivo ajustado salvo em: {CAMINHO_SAIDA}")

# Execução direta
if __name__ == "__main__":
    ajustar_datas()