@echo off
setlocal enabledelayedexpansion

set BASE_DIR=%~dp0
pushd %BASE_DIR%

for %%F in (config\F*_Format*.ini) do (
    echo Running %%F...
    python main.py --config "%%F"
    if errorlevel 1 (
        echo Failed on %%F
        popd
        exit /b 1
    )
)

popd
echo All configurations completed.
endlocal
