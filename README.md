# 📊 Pipeline_ajustes_atuarial

## Projeto voltado para o tratamento, ajuste e qualificação de bases atuariais do RPPS do Município de São Paulo, com foco em servidores ativos, aposentados e pensionistas. O pipeline realiza validações, correções e ajustes conforme regras de negócio e legislação vigente, visando entregar produtos finais consistentes e auditáveis.

### 🗂️ Estrutura de Diretórios
    pipeline_ajustes_atuarial/
    ├── .raw/                  # Dados originais (.xlsx)
    ├── .silver/               # Dados intermediários tratados (Parquet)
    │   ├── .silver_aposentados/
    │   └── .silver_servidor/
    ├── gold/                  # Dados finais ajustados (.xlsx)
    ├── iconfig/               # Arquivos de configuração (YAML/JSON)
    ├── logs/                  # Logs de execução e auditoria
    ├── scripts/               # Scripts de tratamento por tipo de base
    │   ├── scripts_servidor/
    │   └── scripts_aposentados/
    ├── venv/                  # Ambiente virtual Python
    ├── README.md              # Documentação do projeto
    ├── requirements.txt       # Dependências do projeto


## ⚙️ Etapas do Pipeline

## Etapas da Base de Servidor

### step01_servidor.py
    Segrega registros de servidores comissionados sem contribuição e vínculo tipo 4. Esses registros são exportados diretamente para a camada `.gold`, enquanto os demais seguem para a camada `.silver` para processamento posterior.

### step02_servidor.py
    Ajusta o campo `DT_ING_ENTE` para garantir idade mínima de 25 anos no ingresso, com base em `DT_NASC_SERVIDOR`. Se a idade for inferior, a data de ingresso é recalculada. Exibe no terminal os registros alterados e estatísticas de ajuste.

### step03_servidor.py
    Normaliza os campos `DT_ING_SERV_PUB`, `DT_ING_CARREIRA` e `DT_ING_CARGO` com base em `DT_NASC_SERVIDOR`, assegurando idade mínima de 18 anos. Se inferior, os campos são ajustados para `DT_ING_ENTE`. Garante também que `DT_ING_SERV_PUB` não seja posterior a `DT_ING_ENTE`. Exibe os 10 primeiros registros ajustados e estatísticas por campo.

### step04_servidor.py
    Classifica os servidores entre os fundos `FUNPREV (1)` e `FUNFIN (2)` conforme os Decretos Municipais nº 61.151/2022 e nº 64.144/2025:
    - `FUNFIN (2)`: Admitidos até 27/12/2018, nascidos após 28/02/1957, e não aderentes à previdência complementar (`IN_PREV_COMP == "2"`).
    - `FUNPREV (1)`: Demais casos, incluindo os que aderiram ao RPC (`IN_PREV_COMP == "1"`).
    Exibe contagem por tipo de fundo no terminal.

### step05_servidor.py
    Ajusta os campos `VL_BASE_CALCULO` e `VL_REMUNERACAO` conforme critérios:
    - Não podem ser nulos.
    - Devem ser ≥ R$1.518 (salário mínimo).
    - Devem ser ≤ `VL_TETO_ESPECIFICO`.
    Exibe estatísticas de ajustes por tipo de valor.

### step06_servidor.py
    Recalcula `VL_CONTRIBUICAO` como 14% de `VL_BASE_CALCULO`. Se `VL_BASE_CALCULO == SAL_MINIMO`, então `VL_CONTRIBUICAO = 0`. Exibe quantidade de registros ajustados e os 10 primeiros exemplos.

### step07_servidor.py
    Realiza ajustes complementares:
    - Preenche `CO_CRITERIO_ELEGIBILIDADE` com 1 se estiver vazio.
    - Ajusta `CO_PODER` e `CO_TIPO_PODER` com base em `NO_ORGAO`, incluindo TCM como tipo 1.
    - Limita `NU_TEMPO_RGPS` ao teto atuarial de 22.280 meses.
    Exibe estatísticas de ajustes por campo.

### step08_servidor.py
    Exporta o resultado final da base de servidores da camada `.silver` para `.xlsx` na camada `.gold`, consolidando os dados tratados. Remove colunas auxiliares `IDADE_ORIGINAL` e `IDADE_AJUSTADA`. Exibe total de registros e colunas exportadas.

### integrador_servidor.py
    Executa todos os steps sequencialmente, com registro de logs via `loguru`. Em caso de erro, interrompe a execução e registra no arquivo `logs/integrador_servidor.log`.


## 🧾 Etapas da Base de Aposentados

### step01_aposentados.py
    Carrega e filtra os registros da base original de aposentados, preparando para os ajustes posteriores. Exporta para a camada .silver.

### step02_aposentados.py
    Ajusta campos de datas e garante consistência mínima de idade e ingresso. Aplica regras de validação conforme estrutura da base.

### step03_aposentados.py
    Normaliza campos de ingresso e aposentadoria, garantindo coerência entre datas e idade mínima. Exibe estatísticas de ajustes.

### step04_aposentados.py
    Classifica os aposentados entre fundos FUNPREV (1) e FUNFIN (2) conforme regras de elegibilidade e decretos municipais.

### step05_aposentados.py
    Valida e ajusta os valores de aposentadoria (VL_APOSENTADORIA) conforme limites legais e teto previdenciário. Exibe estatísticas.

### step06_aposentados.py
    Calcula ou ajusta o campo VL_CONTRIBUICAO com base na alíquota de 14% sobre a aposentadoria, respeitando exceções.

### step07_aposentados.py
    Realiza ajustes complementares:
    Ajusta CO_PODER e CO_TIPO_PODER com base em NO_ORGAO
    Ajusta CO_TIPO_APOSENTADORIA se estiver vazio, conforme CO_CONDICAO_APOSENTADO

### step08_aposentados.py
    Exporta o resultado final da base de aposentados da camada .silver para .xlsx na camada .gold. Remove colunas auxiliares e exibe estatísticas.

### integrador_aposentados.py
    Executa todos os steps sequencialmente, com registro de logs via `loguru`. Em caso de erro, interrompe a execução e registra no arquivo `logs/integrador_aposentados.log`.

## ⚙️ Tecnologias Utilizadas

    Python 3.10+
    Pandas e PyArrow (manipulação de dados e Parquet)
    OpenPyXL (leitura/escrita de arquivos Excel)
    pyarrow
    Dateutil (manipulação de datas)
    VSCode (ambiente de desenvolvimento)


## 📅 Plano de Desenvolvimento

## 🧭 Visão Geral
    O projeto é dividido em sprints com entregáveis claros para cada tipo de base (servidores, aposentados e pensionistas). A gestão é feita com foco em entregas incrementais, testes contínuos e documentação colaborativa.
    Durante o desenvolvimento, houve necessidade de reajuste da estrutura do projeto, o que levou à reformulação de todos os sprints.
    Este projeto foi conduzido integralmente por mim, atuando como desenvolvedor e gerente de projeto, com apoio de dois colaboradores que auxiliaram na revisão das regras de negócio e testes em dias alternados.

## 🟢 Sprint 1 – Estrutura Inicial do Projeto (30/09/25 - 02/10/25)
    Criação da estrutura de diretórios por camada (.raw, .silver, .gold)
    Organização modular dos scripts por tipo de base
    Definição do ambiente virtual com venv
    Instalação dos pacotes essenciais: pandas, pyarrow, openpyxl, loguru

## 🟢 Sprint 2 – Pipeline de Servidores (02/10/25 - 08/10/25)
    Desenvolvimento dos scripts step01_servidor.py a step09_servidor.py
    Aplicação das regras de negócio conforme relatório de críticas
    Ajustes em datas, valores, vínculos e critérios de elegibilidade
    Criação do integrador integrator_servidor.py
    Exportação final para gold/servidor_final.xlsx
    Validação dos dados ajustados (116.749 registros)

## 🟢 Sprint 3 – Pipeline de Aposentados (09/10/25 - 11/10/25)
    Desenvolvimento dos scripts step01_aposentados.py a step08_aposentados.py
    Implementação das regras específicas de aposentadoria
    Ajustes em poder, tipo de aposentadoria, valores e contribuições
    Criação do integrador integrator_aposentados.py
    Exportação final para gold/aposentados_final.xlsx
    Validação dos dados ajustados

## 🟡 Sprint 4 – Preparação da Base de Pensionistas (12/10/25 - Em andamento)
    Estruturação dos diretórios e arquivos iniciais
    Definição das regras de negócio para pensionistas
    Início do desenvolvimento dos scripts stepXX_pensionistas.py
    Planejamento de validações específicas para vínculos, cotas e duração

## 🟡 Sprint 5 – Integração e Comparativos
    Criação de scripts de comparação entre bases: servidores x aposentados x pensionistas
    Validação cruzada de campos comuns e regras atuariais
    Identificação de inconsistências e ajustes interbase

## 🔵 Sprint 6 – Documentação e Testes Finais
    Atualização completa do README.md com todas as fases e regras aplicadas
    Criação de notebooks de teste para validação dos pipelines
    Testes de execução completa dos integradores
    Preparação para entrega dos produtos finais


## 📋 Regras de Negócio Aplicadas
🔄 Dependências entre Campos
    As regras de tratamento consideram relações lógicas entre campos, respeitando critérios legais e atuariais:
    - CO_TIPO_FUNDO: Classificação entre FUNPREV (1) e FUNFIN (2) com base nos Decretos Municipais nº 61.151/2022 e nº 64.144/2025:
    - FUNFIN (2): DT_ING_ENTE ≤ 27/12/2018, DT_NASC_SERVIDOR > 28/02/1957, IN_PREV_COMP == "2"
    - FUNPREV (1): Todos os demais casos, incluindo IN_PREV_COMP == "1"
    - DT_ING_SERV_PUB, DT_ING_CARGO, DT_ING_CARREIRA dependem de DT_ING_ENTE
    - DT_ING_ENTE depende de DT_NASC_SERVIDOR
    - VL_CONTRIBUICAO depende de VL_BASE_CALCULO
    - NU_TEMPO_RGPS limitado ao teto atuarial de 22.280 meses

## ⚠️ Gatilhos de Inconsistência

    Os ajustes são aplicados somente em registros que apresentarem inconsistência.
    As validações são realizadas por funções específicas que detectam anomalias antes de aplicar qualquer transformação.
    Registros válidos seguem diretamente para .gold; registros inconsistentes passam por .silver.


## 🧾 Regras Específicas por Tipo de Base

### 🧑‍💼 Servidores Ativos
    CO_TIPO_FUNDO: Registros com código 9 são segregados como servidores cedidos.
    CO_TIPO_PODER: Corrigir para 1 (Administração Direta) quando CO_PODER ≠ 1.
    CO_CRITERIO_ELEGIBILIDADE: Preencher com 1 (Sem Critério Diferenciado) se estiver vazio.
    DT_ING_ENTE: Ajustar para garantir idade mínima de ingresso de 25 anos.
    DT_ING_SERV_PUB, DT_ING_CARREIRA, DT_ING_CARGO: Preencher em branco com base em DT_ING_ENTE.
    VL_BASE_CALCULO, VL_REMUNERACAO: Corrigir valores abaixo do salário mínimo ou acima do teto.
    VL_CONTRIBUICAO: Manter em branco para códigos funcionais 3 ou 11; recalcular como 14% de VL_BASE_CALCULO nos demais casos.
    NU_TEMPO_RGPS: Limitar ao teto de 22.280 meses.

### 🧓 Servidores Aposentados
    CO_TIPO_PODER: Corrigir para 1 (Administração Direta) quando CO_PODER ≠ 1.
    CO_TIPO_APOSENTADORIA: Atribuir 2 (Tempo de Contribuição) ou 4 (Invalidez) conforme tipo.
    DT_ING_ENTE e DT_ING_SERV_PUB: Ajustar para garantir idade mínima de ingresso.
    VL_APOSENTADORIA: Corrigir valores fora dos limites legais.
    VL_CONTRIBUICAO: Preencher com 14% da aposentadoria ou ajustar valores excedentes.

### 👨‍👧 Pensionistas
    CO_TIPO_RELACAO: Preencher em branco com código 6 (Outros).
    DT_INICIO_PENSAO: Ajustar se anterior à data de nascimento do pensionista.
    VL_BENEF_PENSAO: Corrigir valores fora dos limites e redistribuir proporcionalmente.
    VL_TOT_PENSAO: Somar valores individuais por instituidor, respeitando o teto.
    VL_PCT_QUOTA: Recalcular proporcionalmente ao valor individual.
    CO_CONDICAO: Atribuir 2 (Inválido) para filhos inválidos e 1 (Válido) para os demais.
    CO_DURACAO: Atribuir 2 (Temporário) para menores de 21 anos e 1 (Vitalício) para os demais.
    NU_TEMPO_DURACAO: Preencher com tempo restante até completar 21 anos para pensionistas temporários.

## 🚀 Instruções Básicas de Uso

### 📁 Preparação
    Coloque os arquivos .xlsx originais na pasta .raw, seguindo o padrão de nome:

    servidor_AAAA_MM.xlsx
    aposentados_AAAA_MM.xlsx
    pensionistas_AAAA_MM.xlsx

    Certifique-se de que o ambiente virtual está ativado e os pacotes do requirements.txt estão instalados corretamente.
    Os dados ajustados serão salvos em:

    .silver (formato Parquet) para registros intermediários
    .gold (formato Excel) para os resultados finais

    Os logs de execução são registrados em:

    logs/integrador_servidor.log
    logs/integrador_aposentados.log


### 🚀 Execução
Ative o ambiente virtual e execute os scripts:
    venv\Scripts\activate
    .\venv\Scripts\Activate.ps1
    python scripts/integrator_servidor.py
    python scripts/integrator_aposentados.py

### 📌 Observações

    Os arquivos devem seguir o padrão de nome: servidor_AAAA_MM.xlsx, aposentados_AAAA_MM.xlsx, pensionistas_AAAA_MM.xlsx
    O pipeline pode ser expandido com novos stepXX.py conforme regras de negócio
    Um script integrador geral (run_pipeline.py) será criado para executar todas as etapas em sequência


## 📤 Desenvolvedor
    Lucas Alves Gouveia | 
    Diretor Técnico de Divisão | 
    IPREM-SP

### Atualização README.md
    Data: 11/10/2025