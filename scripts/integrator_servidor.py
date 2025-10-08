from loguru import logger
import subprocess
import os

# Configurar o log
logger.add("logs/integrador.log", rotation="1 MB", level="INFO", format="{time} | {level} | {message}")

# Caminho da pasta de scripts
scripts_path = "scripts/scripts_servidor"

# Lista de scripts a serem executados em ordem
scripts = [
    "step01_servidor.py",
    "step02_servidor.py",
    "step03_servidor.py",
    "step04_servidor.py",
    "step05_servidor.py",
    "step06_servidor.py",
    "step07_servidor.py",
    "step08_servidor.py"  # Corrigido aqui!
]

# Executar cada script sequencialmente
for script in scripts:
    script_path = os.path.join(scripts_path, script)
    logger.info(f"Executando script: {script_path}")
    try:
        subprocess.run(["python", script_path], check=True)
        logger.info(f"Script {script} executado com sucesso.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Erro ao executar {script}: {e}")
        break