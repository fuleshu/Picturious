@echo off
setlocal EnableExtensions

if /i "%~1"=="--help" goto :usage
if /i "%~1"=="/?" goto :usage

set "ROOT=%~dp0"
set "ROOT=%ROOT:~0,-1%"
set "NSIS_DIR=%ROOT%\target\release\bundle\nsis"
set "NASM_DIR=%ROOT%\.tools\nasm\nasm-3.01"
set "NASM_EXE=%NASM_DIR%\nasm.exe"
set "INSTALLER="

echo.
echo Picturious release installer build
echo ==================================
echo Root: %ROOT%
echo.

pushd "%ROOT%" >nul || (
    echo ERROR: Could not enter project root.
    exit /b 1
)

where cargo >nul 2>nul
if errorlevel 1 (
    echo ERROR: cargo was not found on PATH.
    echo Install Rust from https://rustup.rs/ and open a new terminal.
    popd >nul
    exit /b 1
)

cargo tauri --version >nul 2>nul
if errorlevel 1 (
    echo ERROR: cargo-tauri was not found.
    echo Install it with:
    echo   cargo install tauri-cli --version "^2"
    popd >nul
    exit /b 1
)

if exist "%NASM_EXE%" (
    set "CMAKE_ASM_NASM_COMPILER=%NASM_EXE%"
    set "PATH=%NASM_DIR%;%PATH%"
    echo Using bundled NASM: %NASM_EXE%
) else (
    echo Bundled NASM not found; using NASM from PATH if available.
)

echo.
echo Current configured versions:
findstr /n /c:"version" Cargo.toml src-tauri\tauri.conf.json
echo.

echo Building release installer...
cargo tauri build --bundles nsis
if errorlevel 1 (
    echo.
    echo ERROR: Installer build failed.
    popd >nul
    exit /b 1
)

echo.
echo Build complete.
if exist "%NSIS_DIR%" (
    for /f "delims=" %%F in ('dir /b /a-d /o-d "%NSIS_DIR%\*.exe" 2^>nul') do (
        if not defined INSTALLER set "INSTALLER=%NSIS_DIR%\%%F"
    )
) else (
    echo ERROR: Expected installer folder was not found:
    echo   %NSIS_DIR%
    popd >nul
    exit /b 1
)

if not defined INSTALLER (
    echo ERROR: Build finished, but no installer .exe was found in:
    echo   %NSIS_DIR%
    popd >nul
    exit /b 1
)

echo Installer artifacts:
dir /b /o-d "%NSIS_DIR%\*.exe"
echo.
echo Upload this installer to the GitHub Release:
echo   %INSTALLER%
echo.
echo SHA-256:
certutil -hashfile "%INSTALLER%" SHA256 | findstr /v /i "hash certutil"

popd >nul
endlocal
exit /b 0

:usage
echo Builds the Picturious release installer with Tauri's NSIS bundler.
echo.
echo Usage:
echo   build-installer.bat
echo.
echo Before running, bump the version in Cargo.toml and src-tauri\tauri.conf.json.
echo The generated setup executable is written to target\release\bundle\nsis.
exit /b 0
