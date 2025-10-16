# Caminho do ambiente virtual
$venvPath = ".\venv"

# Remove o ambiente virtual existente, se houver
if (Test-Path $venvPath) {
    Write-Host "Removendo ambiente virtual existente..."
    Remove-Item -Recurse -Force $venvPath
}

# Cria novo ambiente virtual
Write-Host "Criando novo ambiente virtual..."
python -m venv venv

# Ajusta política de execução temporariamente para ativar o venv
Write-Host "Ajustando política de execução para esta sessão..."
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process -Force

# Ativa o ambiente virtual
Write-Host "Ativando o ambiente virtual..."
. .\venv\Scripts\Activate.ps1

# Instala dependências, se houver requirements.txt
if (Test-Path ".\requirements.txt") {
    Write-Host "Instalando dependências do requirements.txt..."
    pip install -r requirements.txt
} else {
    Write-Host "Arquivo requirements.txt não encontrado. Ambiente criado sem dependências."
}

Write-Host "Ambiente virtual recriado com sucesso!"
