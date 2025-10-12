from loguru import logger
import subprocess
import os

# Configurar o log
logger.add("logs/integrador_aposentados.log", rotation="1 MB", level="INFO", format="{time} | {level} | {message}")

# Caminho da pasta de scripts
scripts_path = "scripts/scripts_aposentados"

# Lista de scripts a serem executados em ordem
scripts = [
    "step01_aposentados.py",
    "step02_aposentados.py",
    "step03_aposentados.py",
    "step04_aposentados.py",
    "step05_aposentados.py",
    "step06_aposentados.py",
    "step07_aposentados.py",
    "step08_aposentados.py"
]

# Caminho do Python do ambiente virtual
python_exec = os.path.join(os.environ['VIRTUAL_ENV'], 'Scripts', 'python.exe')

# Executar cada script sequencialmente
for script in scripts:
    script_path = os.path.join(scripts_path, script)
    logger.info(f"Executando script: {script_path}")
    print(f"▶️ Executando: {script}")
    try:
        subprocess.run([python_exec, script_path], check=True)
        logger.info(f"Script {script} executado com sucesso.")
        print(f"✅ Script {script} executado com sucesso.\n")
    except subprocess.CalledProcessError as e:
        logger.error(f"Erro ao executar {script}: {e}")
        print(f"❌ Erro ao executar {script}: {e}")
        break