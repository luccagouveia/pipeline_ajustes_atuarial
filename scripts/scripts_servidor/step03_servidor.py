import pandas as pd
import os

# Caminhos de entrada e saída
CAMINHO_ENTRADA = os.path.join(".silver", ".silver_servidor", "servidor_step02.parquet")
CAMINHO_SAIDA = os.path.join(".silver", ".silver_servidor", "servidor_step03.parquet")

def ajustar_datas():
    # Carregar o arquivo Parquet da camada .silver
    df = pd.read_parquet(CAMINHO_ENTRADA)

    # Garantir que os campos estejam no formato datetime
    campos_data = ["DT_ING_SERV_PUB", "DT_ING_CARREIRA", "DT_ING_CARGO", "DT_NASC_SERVIDOR", "DT_ING_ENTE"]
    for campo in campos_data:
        df[campo] = pd.to_datetime(df[campo], errors='coerce', dayfirst=True)

    # Armazenar cópias originais para comparação
    df["DT_ING_SERV_PUB_ORIG"] = df["DT_ING_SERV_PUB"]
    df["DT_ING_CARREIRA_ORIG"] = df["DT_ING_CARREIRA"]
    df["DT_ING_CARGO_ORIG"] = df["DT_ING_CARGO"]

    # Ajuste por idade < 18 anos
    def ajustar_idade_minima(dt_ing, dt_nasc, dt_ente):
        if pd.isnull(dt_ing) or pd.isnull(dt_nasc):
            return dt_ing
        idade = (dt_ing - dt_nasc).days // 365
        if idade < 18:
            return dt_ente
        return dt_ing

    df["DT_ING_SERV_PUB"] = df.apply(lambda row: ajustar_idade_minima(row["DT_ING_SERV_PUB"], row["DT_NASC_SERVIDOR"], row["DT_ING_ENTE"]), axis=1)
    df["DT_ING_CARREIRA"] = df.apply(lambda row: ajustar_idade_minima(row["DT_ING_CARREIRA"], row["DT_NASC_SERVIDOR"], row["DT_ING_ENTE"]), axis=1)
    df["DT_ING_CARGO"] = df.apply(lambda row: ajustar_idade_minima(row["DT_ING_CARGO"], row["DT_NASC_SERVIDOR"], row["DT_ING_ENTE"]), axis=1)

    # Regra adicional: DT_ING_SERV_PUB não pode ser maior que DT_ING_ENTE
    df["DT_ING_SERV_PUB"] = df.apply(lambda row: min(row["DT_ING_SERV_PUB"], row["DT_ING_ENTE"]) if pd.notnull(row["DT_ING_SERV_PUB"]) and pd.notnull(row["DT_ING_ENTE"]) else row["DT_ING_SERV_PUB"], axis=1)

    # Contagem de registros
    total_original = len(df)

    # Identificar registros ajustados
    df_ajustados = df[(df["DT_ING_SERV_PUB"] != df["DT_ING_SERV_PUB_ORIG"]) |
                      (df["DT_ING_CARREIRA"] != df["DT_ING_CARREIRA_ORIG"]) |
                      (df["DT_ING_CARGO"] != df["DT_ING_CARGO_ORIG"])]

    # Contagem de ajustes por campo
    qtd_sp = (df["DT_ING_SERV_PUB"] != df["DT_ING_SERV_PUB_ORIG"]).sum()
    qtd_cr = (df["DT_ING_CARREIRA"] != df["DT_ING_CARREIRA_ORIG"]).sum()
    qtd_cg = (df["DT_ING_CARGO"] != df["DT_ING_CARGO_ORIG"]).sum()

    # Exibir os 10 primeiros registros ajustados no terminal
    print("\n🔍 Registros ajustados (até 10 linhas):")
    print(df_ajustados[[
        "ID_SERVIDOR_MATRICULA",
        "DT_ING_ENTE",
        "DT_ING_SERV_PUB_ORIG", "DT_ING_SERV_PUB",
        "DT_ING_CARREIRA_ORIG", "DT_ING_CARREIRA",
        "DT_ING_CARGO_ORIG", "DT_ING_CARGO"
    ]].head(10).to_string(index=False))

    # Exibir contagem de alterações
    print("\n📊 Resumo das alterações:")
    print(f"→ DT_ING_SERV_PUB ajustado em {qtd_sp} registros")
    print(f"→ DT_ING_CARREIRA ajustado em {qtd_cr} registros")
    print(f"→ DT_ING_CARGO ajustado em {qtd_cg} registros")

    # Salvar o resultado ajustado
    df.drop(columns=["DT_ING_SERV_PUB_ORIG", "DT_ING_CARREIRA_ORIG", "DT_ING_CARGO_ORIG"], inplace=True)
    df.to_parquet(CAMINHO_SAIDA, index=False)

    # Verificação final
    total_ajustado = len(df)
    print(f"\n✅ Total de registros no arquivo original: {total_original}")
    print(f"✅ Total de registros no arquivo ajustado: {total_ajustado}")
    print(f"📁 Arquivo salvo em: {CAMINHO_SAIDA}")

# Execução direta
if __name__ == "__main__":
    ajustar_datas()