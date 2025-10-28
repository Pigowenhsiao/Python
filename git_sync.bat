@echo off
SETLOCAL

:: --- Check Prerequisites ---
where git >nul 2>nul
if %errorlevel% neq 0 (
    echo ----------------------------------------------------
    echo ERROR: Git command not found. Please ensure Git is installed and in PATH.
    echo ----------------------------------------------------
    pause
    GOTO :EOF
)

echo.
echo ====================================================
echo 🚀 Initiating Project Push Procedure (Kaizen-TDS)
echo ====================================================
echo.

:: --- Define Subdirectories for potential submodule processing ---
:: (These are large directories and are assumed to contain content that needs staging)
SET "SUBMODULES=001_GRATING 002_MESA 003_N-electrode 004_T2-EML 005_BJ1 006_BJ1 007_BJ2 008_WG-EML 009_GRATING 010_GRATING 011_MESA 012_MESA 013_MESA 014_PIX 015_P-electrode 016_P-electrode 017_GRATING 018_T2-DML 019_MESA 020_MESA 021_MESA 022_P-electrode 023_P-electrode 024_ISO-EML 025_PIX 026_PIX 027_MESA 028_TH-DML 029_TH-DML 030_N-electrode 031_SEM-EML 032_SEM-DML 033_N-electrode 034_N-electrode 035_N-electrode 036_N-electrode 037_T1-DML 038_EA-EML 039_Ru-EML 040_LD-EML 041_T-CVD 042_PIX-DML 043_LD-SPUT 044_EA-WG_LD_WG 045_Ru_AFM 046_Banchi-IV 048_TAK_SPC 049_TAK_PLX 050_TAK_MESA 051_Particle 052_Facet_THK"

echo 1. Checking and committing internal changes in submodules/large folders...
echo.

for %%d in (%SUBMODULES%) do (
    if exist "%%d\.git" (
        echo   ^-> Entering submodule: %%d
        cd "%%d"
        git add .
        git commit -m "Auto sync submodule: %%d updates"
        cd ..
    )
)
echo Submodule check complete.
echo.

:: --- Stage All Changes ---
echo 2. Staging all modified, new, and deleted files...
git add .
if %errorlevel% neq 0 (
    echo FATAL ERROR: Failed to stage files.
    pause
    GOTO :EOF
)

:: --- Check if changes exist for commit ---
git diff-index --quiet HEAD
if %errorlevel% equ 0 (
    echo STATUS: No new changes detected. Skipping new commit.
    GOTO Push
)

:: --- Create Commit ---
SET /P commit_msg="3. Enter Commit Message (e.g., Daily Sync): "
if "%commit_msg%"=="" SET commit_msg="Auto daily sync from Windows"

git commit -m "%commit_msg%"
if %errorlevel% neq 0 (
    echo FATAL ERROR: Commit failed.
    pause
    GOTO :EOF
)
echo Commit successful: "%commit_msg%"

:Push
:: --- Execute Push ---
echo.
echo 4. Pushing changes to remote 'python' (Branch: master)...
git push -u python main
if %errorlevel% neq 0 (
    echo ----------------------------------------------------
    echo ❌ PUSH FAILED! Check network or PAT token validity.
    echo ----------------------------------------------------
    pause
    GOTO :EOF
)

echo.
echo ====================================================
echo ✅ PUSH SUCCESSFUL! Project is synced to GitHub.
echo ====================================================
pause

ENDLOCAL