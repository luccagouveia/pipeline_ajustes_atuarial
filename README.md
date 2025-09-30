# 📊 pipeline_ajustes_atuarial

Este projeto tem como objetivo estruturar e automatizar o tratamento de inconsistências cadastrais nas bases de dados do RPPS (Regime Próprio de Previdência Social), envolvendo pensionistas, servidores aposentados e servidores ativos. O pipeline é organizado em camadas de dados e utiliza scripts em Python e PySpark para realizar os ajustes conforme regras atuariais e legais.

---

## 🗂️ Estrutura de Diretórios

pipeline_ajustes_atuarial/
├── .raw/                  # Dados originais (XLSX)
├── .silver/               # Dados tratados parcialmente
├── gold/                  # Dados finais prontos para consumo
├── scripts/               # Scripts de processamento
│   ├── raw_to_silver/
│   ├── silver_to_gold/
│   └── raw_to_gold/
├── iconfig/               # Parâmetros e regras de negócio
│   └── parametros.yaml
├── logs/                  # Logs de execução
├── notebooks/             # Jupyter notebooks para testes
├── venv/                  # Ambiente virtual Python
├── requirements.txt       # Dependências do projeto
└── README.md              # Documentação

---

## ⚙️ Tecnologias Utilizadas

- Python 3.10+
- Pandas
- PySpark (opcional)
- Openpyxl
- Pyarrow
- Loguru
- JupyterLab
- VSCode
- Windows + PowerShell/WSL

---

## 📅 Plano de Desenvolvimento – 7 Fases

### ✅ Fase 1 – Estruturação do Ambiente (concluída)
- Criação da estrutura de diretórios

### ✅ Fase 2 – Criação do Ambiente Virtual (concluída)
- Criação do ambiente com `venv`
- Instalação de pacotes essenciais

### 🔄 Fase 3 – Definição de Parâmetros e Regras
- Criar `iconfig/parametros.yaml`
- Documentar regras de negócio por tipo de base

### 🔄 Fase 4 – Scripts de Leitura e Validação Inicial
- Leitura dos arquivos `.xlsx`
- Validação de estrutura e campos obrigatórios
- Geração de logs de inconsistência

### 🔄 Fase 5 – Tratamentos e Ajustes
- Implementar regras de tratamento por base
- Salvar resultados na camada `.silver`

### 🔄 Fase 6 – Geração da Camada Gold
- Consolidar dados tratados
- Aplicar filtros finais e exportar para consumo

### 🔄 Fase 7 – Documentação e Testes
- Finalizar `README.md`
- Criar notebooks de teste
- Validar execução completa do pipeline

---

## 📋 Regras de Negócio Aplicadas (Definição Inicial)
    As regras de negócio definidas para este pipeline têm como objetivo garantir a consistência, integridade e conformidade legal dos dados atuariais do RPPS. Os tratamentos serão aplicados conforme o tipo de base (pensionistas, aposentados, servidores ativos), respeitando os critérios legais e metodológicos vigentes.

    ⚠️ Os scripts de tratamento ainda serão desenvolvidos com base nos arquivos reais que serão inseridos na próxima fase.


    🧾 Pensionistas

    CO_TIPO_RELACAO: Preencher registros em branco com código 6 – Outros.
    DT_INICIO_PENSAO: Ajustar datas que sejam anteriores à data de nascimento do pensionista.
    VL_BENEF_PENSAO: Identificar valores abaixo do salário mínimo ou acima do teto; redistribuir proporcionalmente entre pensionistas do mesmo instituidor.
    VL_TOT_PENSAO: Calcular somando os valores individuais por instituidor, respeitando limites legais.
    VL_PCT_QUOTA: Recalcular proporcionalmente ao valor individual da pensão.
    CO_CONDICAO: Atribuir 2 – Inválido para filhos inválidos e 1 – Válido para os demais.
    CO_DURACAO: Atribuir 2 – Temporário para menores de 21 anos e 1 – Vitalício para os demais.
    NU_TEMPO_DURACAO: Preencher com o tempo restante (em anos) até completar 21 anos para pensionistas temporários.


    🧾 Servidores Aposentados

    CO_TIPO_PODER: Corrigir registros com CO_PODER diferente de 1 para CO_TIPO_PODER = 1 – Administração Direta.
    CO_TIPO_APOSENTADORIA: Atribuir 2 – Tempo de Contribuição para aposentadorias válidas e 4 – Invalidez para inválidas.
    DT_ING_SERV_PUB / DT_ING_ENTE: Ajustar datas para garantir idade mínima de ingresso de 25 anos.
    VL_APOSENTADORIA: Corrigir valores abaixo do salário mínimo ou acima do teto específico.
    VL_CONTRIBUICAO: Preencher com 14% da aposentadoria ou ajustar valores excedentes.


    🧾 Servidores Ativos

    CO_TIPO_FUNDO: Registros com código 9 serão segregados em aba específica (servidores cedidos).
    CO_TIPO_PODER: Corrigir para 1 – Administração Direta quando CO_PODER for diferente de 1.
    CO_CRITERIO_ELEGIBILIDADE: Preencher com 1 – Sem Critério Diferenciado para registros em branco.
    DT_ING_SERV_PUB / DT_ING_ENTE: Ajustar para garantir idade mínima de ingresso de 25 anos.
    DT_ING_CARREIRA / DT_ING_CARGO: Preencher registros em branco com a data ajustada de DT_ING_ENTE.
    VL_BASE_CALCULO / VL_REMUNERACAO: Corrigir valores abaixo do salário mínimo ou acima do teto.
    VL_CONTRIBUICAO: Manter em branco para servidores com códigos funcionais 3 ou 11.
    NU_TEMPO_RGPS: Limitar tempo de contribuição ao teto atuarial de 22.280 meses.
    
---

## ▶️ Instruções Básicas de Uso

1. Ative o ambiente virtual:
   ```cmd
   venv\Scripts\activate.bat

2. Instale os pacotes:
    'pip install -r requirements.txt

