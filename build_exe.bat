@echo off
:: ============================================================
:: build_exe.bat — Gera o executável RFB_Crawler.exe
:: ============================================================
:: Pré-requisito: venv já criada com as dependências instaladas.
:: Execute este script UMA VEZ para gerar o .exe.
:: O executável ficará em:  dist\RFB_Crawler.exe
:: ============================================================

echo.
echo === RFB Crawler — Gerando executavel ===
echo.

:: Ativa o ambiente virtual
call venv\Scripts\activate.bat

:: Instala o PyInstaller caso ainda nao esteja presente
pip show pyinstaller >nul 2>&1 || pip install pyinstaller

:: Limpa builds anteriores
if exist build   rmdir /s /q build
if exist dist    rmdir /s /q dist

:: Gera o executável usando o spec file
pyinstaller rfb_crawler.spec

if %errorlevel% neq 0 (
    echo.
    echo [ERRO] Falha ao gerar o executavel. Verifique as mensagens acima.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo  Executavel gerado com sucesso!
echo  Caminho: dist\RFB_Crawler.exe
echo.
echo  Copie dist\RFB_Crawler.exe para a area de trabalho
echo  dos usuarios. Nao e necessario ter Python instalado.
echo ============================================================
echo.
pause
