import pandas as pd
import os

def executar():
    # Caminhos de entrada e saída
    caminho_entrada = os.path.join('.silver', '.silver_pensionistas', 'step01_pensionistas.parquet')
    caminho_saida = os.path.join('.silver', '.silver_pensionistas', 'step02_pensionistas.parquet')

    # Verificar se o arquivo de entrada existe
    if not os.path.exists(caminho_entrada):
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_entrada}")

    # Garantir que o diretório de saída exista
    os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)

    print(f"Processando arquivo: {os.path.basename(caminho_entrada)}")

    # Carregar o arquivo original
    df = pd.read_parquet(caminho_entrada)

    # Contar registros originais
    total_original = len(df)

    # Converter campos para datetime
    df['DT_INICIO_PENSAO'] = pd.to_datetime(df['DT_INICIO_PENSAO'], errors='coerce')
    df['DT_NASC_PENSIONISTA'] = pd.to_datetime(df['DT_NASC_PENSIONISTA'], errors='coerce')

    # Criar cópia para ajuste
    df_ajustado = df.copy()

    # Identificar registros com DT_INICIO_PENSAO anterior à DT_NASC_PENSIONISTA
    mask_data_invalida = df_ajustado['DT_INICIO_PENSAO'] < df_ajustado['DT_NASC_PENSIONISTA']

    # Ajustar DT_INICIO_PENSAO para DT_NASC_PENSIONISTA
    df_ajustado.loc[mask_data_invalida, 'DT_INICIO_PENSAO'] = df_ajustado.loc[mask_data_invalida, 'DT_NASC_PENSIONISTA']

    # Salvar o resultado ajustado
    df_ajustado.to_parquet(caminho_saida, index=False)

    # Contar registros ajustados
    total_ajustado = len(df_ajustado)

    print(f"Total de registros no arquivo original: {total_original}")
    print(f"Total de registros no arquivo ajustado: {total_ajustado}")
    print(f"Registros ajustados (DT_INICIO_PENSAO < DT_NASC_PENSIONISTA): {mask_data_invalida.sum()}")
    print(f"Arquivo salvo em: {caminho_saida}")

    # Verificação: mostrar os 10 primeiros registros ajustados
    df_verificacao = pd.DataFrame({
        'DT_NASC_PENSIONISTA': df.loc[mask_data_invalida, 'DT_NASC_PENSIONISTA'],
        'DT_INICIO_PENSAO_ORIGINAL': df.loc[mask_data_invalida, 'DT_INICIO_PENSAO'],
        'DT_INICIO_PENSAO_AJUSTADO': df_ajustado.loc[mask_data_invalida, 'DT_INICIO_PENSAO']
    })

    print("\n🔍 Registros ajustados (até 10 linhas):")
    print(df_verificacao.head(10))

# Executar se chamado diretamente
if __name__ == "__main__":
    executar()