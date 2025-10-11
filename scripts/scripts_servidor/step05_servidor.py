import pandas as pd

# Constante do salário mínimo
SAL_MINIMO = 1518

# Caminhos dos arquivos
input_path = ".silver/.silver_servidor/servidor_step04.parquet"
output_path = ".silver/.silver_servidor/servidor_step05.parquet"

def ajustar_valores():
    # Carregar o arquivo de entrada
    df = pd.read_parquet(input_path)

    # Contar registros originais
    total_original = len(df)

    # Contadores de ajustes
    ajustes_base_menor_minimo = ((df["VL_BASE_CALCULO"] < SAL_MINIMO) | (df["VL_BASE_CALCULO"].isna())).sum()
    ajustes_base_maior_teto = (df["VL_BASE_CALCULO"] > df["VL_TETO_ESPECIFICO"]).sum()

    ajustes_remu_menor_minimo = ((df["VL_REMUNERACAO"] < SAL_MINIMO) | (df["VL_REMUNERACAO"].isna())).sum()
    ajustes_remu_maior_teto = (df["VL_REMUNERACAO"] > df["VL_TETO_ESPECIFICO"]).sum()

    # Aplicar ajustes em VL_BASE_CALCULO
    df.loc[df["VL_BASE_CALCULO"].isna() | (df["VL_BASE_CALCULO"] < SAL_MINIMO), "VL_BASE_CALCULO"] = SAL_MINIMO
    df.loc[df["VL_BASE_CALCULO"] > df["VL_TETO_ESPECIFICO"], "VL_BASE_CALCULO"] = df["VL_TETO_ESPECIFICO"]

    # Aplicar ajustes em VL_REMUNERACAO
    df.loc[df["VL_REMUNERACAO"].isna() | (df["VL_REMUNERACAO"] < SAL_MINIMO), "VL_REMUNERACAO"] = SAL_MINIMO
    df.loc[df["VL_REMUNERACAO"] > df["VL_TETO_ESPECIFICO"], "VL_REMUNERACAO"] = df["VL_TETO_ESPECIFICO"]

    # Salvar o resultado no arquivo de saída
    df.to_parquet(output_path, index=False)

    # Contar registros ajustados
    total_ajustado = len(df)

    # Imprimir os resultados
    print(f"Ajustes em VL_BASE_CALCULO:")
    print(f" - Menores que SAL_MINIMO: {ajustes_base_menor_minimo}")
    print(f" - Maiores que VL_TETO_ESPECIFICO: {ajustes_base_maior_teto}")

    print(f"Ajustes em VL_REMUNERACAO:")
    print(f" - Menores que SAL_MINIMO: {ajustes_remu_menor_minimo}")
    print(f" - Maiores que VL_TETO_ESPECIFICO: {ajustes_remu_maior_teto}")

    print(f"\n✅ Total de registros no arquivo original: {total_original}")
    print(f"✅ Total de registros no arquivo ajustado: {total_ajustado}")
    print(f"📁 Arquivo salvo em: {output_path}")

# Execução direta
if __name__ == "__main__":
    ajustar_valores()