@echo off
echo ============================================
echo  Setup - Transparencia Servidores ETL
echo ============================================
echo.

echo [1/3] Criando ambiente virtual (.venv)...
python -m venv .venv
if errorlevel 1 (
    echo ERRO: Falha ao criar ambiente virtual. Verifique se o Python esta instalado.
    pause
    exit /b 1
)

echo [2/3] Ativando ambiente virtual...
call .venv\Scripts\activate.bat

echo [3/3] Instalando dependencias...
pip install -r requirements.txt
if errorlevel 1 (
    echo ERRO: Falha ao instalar dependencias.
    pause
    exit /b 1
)

echo.
echo ============================================
echo  Setup concluido com sucesso!
echo  Para ativar o ambiente: .venv\Scripts\activate
echo  Para executar: python -m src.main
echo ============================================
pause
