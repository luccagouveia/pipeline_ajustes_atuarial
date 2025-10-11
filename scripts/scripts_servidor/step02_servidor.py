import pandas as pd
import os
from datetime import datetime

def executar():
    # Caminhos de entrada e saída
    caminho_entrada = os.path.join('.silver', '.silver_servidor', 'servidor_step01.parquet')
    caminho_saida = os.path.join('.silver', '.silver_servidor', 'servidor_step02.parquet')

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
    df['DT_ING_ENTE'] = pd.to_datetime(df['DT_ING_ENTE'], errors='coerce')
    df['DT_NASC_SERVIDOR'] = pd.to_datetime(df['DT_NASC_SERVIDOR'], errors='coerce')

    # Calcular idade original
    df['IDADE_ORIGINAL'] = (df['DT_ING_ENTE'] - df['DT_NASC_SERVIDOR']).dt.days // 365

    # Criar cópia para ajuste
    df_ajustado = df.copy()

    # Identificar registros com idade < 18
    mask_idade_menor_que_18 = df_ajustado['IDADE_ORIGINAL'] < 18

    # Ajustar DT_ING_ENTE para DT_NASC_SERVIDOR + 25 anos, limitado a 2024
    dt_limite = pd.Timestamp('2024-12-31')
    novos_dt_ing_ente = df_ajustado.loc[mask_idade_menor_que_18, 'DT_NASC_SERVIDOR'] + pd.DateOffset(years=25)
    novos_dt_ing_ente = novos_dt_ing_ente.where(novos_dt_ing_ente <= dt_limite, dt_limite)
    df_ajustado.loc[mask_idade_menor_que_18, 'DT_ING_ENTE'] = novos_dt_ing_ente

    # Recalcular idade ajustada
    df_ajustado['IDADE_AJUSTADA'] = (df_ajustado['DT_ING_ENTE'] - df_ajustado['DT_NASC_SERVIDOR']).dt.days // 365

    # Salvar o resultado ajustado
    df_ajustado.to_parquet(caminho_saida, index=False)

    # Contar registros ajustados
    total_ajustado = len(df_ajustado)

    print(f"Total de registros no arquivo original: {total_original}")
    print(f"Total de registros no arquivo ajustado: {total_ajustado}")
    print(f"Registros ajustados: {mask_idade_menor_que_18.sum()}")
    print(f"Arquivo salvo em: {caminho_saida}")

    # Verificação: mostrar os 10 primeiros registros ajustados
    df_verificacao = pd.DataFrame({
        'DT_NASC_SERVIDOR': df.loc[mask_idade_menor_que_18, 'DT_NASC_SERVIDOR'],
        'DT_ING_ENTE_ORIGINAL': df.loc[mask_idade_menor_que_18, 'DT_ING_ENTE'],
        'IDADE_ORIGINAL': df.loc[mask_idade_menor_que_18, 'IDADE_ORIGINAL'],
        'DT_ING_ENTE_AJUSTADO': df_ajustado.loc[mask_idade_menor_que_18, 'DT_ING_ENTE'],
        'IDADE_AJUSTADA': df_ajustado.loc[mask_idade_menor_que_18, 'IDADE_AJUSTADA']
    })

    print("\n🔍 Registros ajustados (até 10 linhas):")
    print(df_verificacao.head(10))

# Executar se chamado diretamente
if __name__ == "__main__":
    executar()