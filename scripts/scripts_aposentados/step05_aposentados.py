import pandas as pd
import os

# Constante do salário mínimo
SAL_MINIMO = 1518

# Caminhos dos arquivos
input_path = ".silver/.silver_aposentados/step04_aposentados.parquet"
output_path = ".silver/.silver_aposentados/step05_aposentados.parquet"

def ajustar_valores():
    # Carregar o arquivo de entrada
    df = pd.read_parquet(input_path)

    # Contar registros originais
    total_original = len(df)

    # Contadores de ajustes
    ajustes_menor_minimo = ((df["VL_APOSENTADORIA"] < SAL_MINIMO) | (df["VL_APOSENTADORIA"].isna())).sum()
    ajustes_maior_teto = (df["VL_APOSENTADORIA"] > df["VL_TETO_ESPECIFICO"]).sum()

    # Aplicar ajustes
    df.loc[df["VL_APOSENTADORIA"].isna() | (df["VL_APOSENTADORIA"] < SAL_MINIMO), "VL_APOSENTADORIA"] = SAL_MINIMO
    df.loc[df["VL_APOSENTADORIA"] > df["VL_TETO_ESPECIFICO"], "VL_APOSENTADORIA"] = df["VL_TETO_ESPECIFICO"]

    # Salvar o resultado no arquivo de saída
    df.to_parquet(output_path, index=False)

    # Contar registros ajustados
    total_ajustado = len(df)

    # Imprimir os resultados
    print(f"Ajustes em VL_APOSENTADORIA:")
    print(f" - Menores que SAL_MINIMO ou nulos: {ajustes_menor_minimo}")
    print(f" - Maiores que VL_TETO_ESPECIFICO: {ajustes_maior_teto}")
    total_ajustados = ajustes_menor_minimo + ajustes_maior_teto
    print(f"\n📊 Total de registros ajustados: {total_ajustados}")

    print(f"\n✅ Total de registros no arquivo original: {total_original}")
    print(f"✅ Total de registros no arquivo ajustado: {total_ajustado}")
    print(f"📁 Arquivo salvo em: {output_path}")

# Execução direta
if __name__ == "__main__":
    ajustar_valores()