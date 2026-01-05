@echo off

cd /d %~dp0
cd ../

rem プログラムデータのクローン

git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\001_GRATING\10_Python program" 001_GRATING
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\002_MESA\10_Python program" 002_MESA
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\003_N-electrode\10_Python program" 003_N-electrode
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\004_T2-EML\10_Python program" 004_T2-EML
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\005_BJ1\10_Python program" 005_BJ1
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\006_BJ1\10_Python program" 006_BJ1
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\007_BJ2\10_Python program" 007_BJ2
mkdir 008_WG-EML
cd ./008_WG-EML
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\008_WG-EML\F1\10_Python program" F1
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\008_WG-EML\F2\10_Python program" F2
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\008_WG-EML\F6\10_Python program" F6
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\008_WG-EML\F7\10_Python program" F7
cd ../
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\009_GRATING\10_Python program" 009_GRATING
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\010_GRATING\10_Python program" 010_GRATING
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\011_MESA\10_Python program" 011_MESA
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\012_MESA\10_Python program" 012_MESA
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\013_MESA\10_Python program" 013_MESA
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\014_PIX\10_Python program" 014_PIX
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\015_P-electrode\10_Python program" 015_P-electrode
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\016_P-electrode\10_Python program" 016_P-electrode
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\017_GRATING\10_Python program" 017_GRATING
mkdir 018_T2-DML
cd ./018_T2-DML
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\018_T2-DML\F5-10G\10_Python program" F5-10G
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\018_T2-DML\F5-25G\10_Python program" F5-25G
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\018_T2-DML\F6-10G\10_Python program" F6-10G
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\018_T2-DML\F6-25G\10_Python program" F6-25G
cd ../
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\019_MESA\10_Python program" 019_MESA
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\020_MESA\10_Python program" 020_MESA
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\021_MESA\10_Python program" 021_MESA
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\022_P-electrode\10_Python program" 022_P-electrode
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\023_P-electrode\10_Python program" 023_P-electrode
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\024_ISO-EML\10_Python program" 024_ISO-EML
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\025_PIX\10_Python program" 025_PIX
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\026_PIX\10_Python program" 026_PIX
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\027_MESA\10_Python program" 027_MESA
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\028_TH-DML\10_Python program" 028_TH-DML
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\029_TH-DML\10_Python program" 029_TH-DML
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\030_N-electrode\10_Python program" 030_N-electrode
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\031_SEM-EML\10_Python program" 031_SEM-EML
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\032_SEM-DML\10_Python program" 032_SEM-DML
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\033_N-electrode\10_Python program" 033_N-electrode
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\034_N-electrode\10_Python program" 034_N-electrode
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\035_N-electrode\10_Python program" 035_N-electrode
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\036_N-electrode\10_Python program" 036_N-electrode
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\037_T1-DML\10_Python program" 037_T1-DML
mkdir 038_EA-EML
cd ./038_EA-EML
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\038_EA-EML\F1\10_Python program" F1
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\038_EA-EML\F7\10_Python program" F7
cd ../
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\039_Ru-EML\10_Python program" 039_Ru-EML
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\040_LD-EML\10_Python program" 040_LD-EML
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\041_T-CVD\10_Python program" 041_T-CVD
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\042_PIX-DML\10_Python program" 042_PIX-DML
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\999_共通プログラム" MyModule
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\10_JMP\SQL.git" SQL
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\10_JMP\SQL_Program" SQL_Program
git clone "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\OtherPrograms" OtherPrograms




rem プログラムに使用するテキストファイルのコピー


copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\001_GRATING\13_ProgramUsedFile\*.txt" "001_GRATING\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\002_MESA\13_ProgramUsedFile\*.txt" "002_MESA\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\004_T2-EML\13_ProgramUsedFile\*.txt" "004_T2-EML\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\005_BJ1\13_ProgramUsedFile\*.txt" "005_BJ1\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\006_BJ1\13_ProgramUsedFile\*.txt" "006_BJ1\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\007_BJ2\13_ProgramUsedFile\*.txt" "007_BJ2\"
cd ./008_WG-EML
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\008_WG-EML\F1\13_ProgramUsedFile\*.txt" "F1\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\008_WG-EML\F2\13_ProgramUsedFile\*.txt" "F2\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\008_WG-EML\F6\13_ProgramUsedFile\*.txt" "F6\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\008_WG-EML\F7\13_ProgramUsedFile\*.txt" "F7\"
cd ../
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\009_GRATING\13_ProgramUsedFile\*.txt" "009_GRATING\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\010_GRATING\13_ProgramUsedFile\*.txt" "010_GRATING\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\011_MESA\13_ProgramUsedFile\*.txt" "011_MESA\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\012_MESA\13_ProgramUsedFile\*.txt" "012_MESA\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\013_MESA\13_ProgramUsedFile\*.txt" "013_MESA\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\014_PIX\13_ProgramUsedFile\*.txt" "014_PIX\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\015_P-electrode\13_ProgramUsedFile\*.txt" "015_P-electrode\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\016_P-electrode\13_ProgramUsedFile\*.txt" "016_P-electrode\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\017_GRATING\13_ProgramUsedFile\*.txt" "017_GRATING\"
cd ./018_T2-DML
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\018_T2-DML\F5-10G\13_ProgramUsedFile\*.txt" "F5-10G\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\018_T2-DML\F5-25G\13_ProgramUsedFile\*.txt" "F5-25G\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\018_T2-DML\F6-10G\13_ProgramUsedFile\*.txt" "F6-10G\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\018_T2-DML\F6-25G\13_ProgramUsedFile\*.txt" "F6-25G\"
cd ../
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\019_MESA\13_ProgramUsedFile\*.txt" "019_MESA\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\020_MESA\13_ProgramUsedFile\*.txt" "020_MESA\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\021_MESA\13_ProgramUsedFile\*.txt" "021_MESA\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\022_P-electrode\13_ProgramUsedFile\*.txt" "022_P-electrode\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\023_P-electrode\13_ProgramUsedFile\*.txt" "023_P-electrode\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\024_ISO-EML\13_ProgramUsedFile\*.txt" "024_ISO-EML\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\025_PIX\13_ProgramUsedFile\*.txt" "025_PIX\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\026_PIX\13_ProgramUsedFile\*.txt" "026_PIX\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\027_MESA\13_ProgramUsedFile\*.txt" "027_MESA\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\028_TH-DML\13_ProgramUsedFile\*.txt" "028_TH-DML\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\029_TH-DML\13_ProgramUsedFile\*.txt" "029_TH-DML\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\030_N-electrode\13_ProgramUsedFile\*.txt" "030_N-electrode\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\031_SEM-EML\13_ProgramUsedFile\*.txt" "031_SEM-EML\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\032_SEM-DML\13_ProgramUsedFile\*.txt" "032_SEM-DML\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\033_N-electrode\13_ProgramUsedFile\*.txt" "033_N-electrode\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\034_N-electrode\13_ProgramUsedFile\*.txt" "034_N-electrode\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\035_N-electrode\13_ProgramUsedFile\*.txt" "035_N-electrode\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\036_N-electrode\13_ProgramUsedFile\*.txt" "036_N-electrode\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\037_T1-DML\13_ProgramUsedFile\*.txt" "037_T1-DML\"
cd ./038_EA-EML
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\038_EA-EML\F1\13_ProgramUsedFile\*.txt" "F1\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\038_EA-EML\F7\13_ProgramUsedFile\*.txt" "F7\"
cd ../
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\039_Ru-EML\13_ProgramUsedFile\*.txt" "039_Ru-EML\"
cd ./040_LD-EML
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\040_LD-EML\F1\13_ProgramUsedFile\*.txt" "F1\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\040_LD-EML\F2\13_ProgramUsedFile\*.txt" "F2\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\040_LD-EML\F6\13_ProgramUsedFile\*.txt" "F6\"
cd ../
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\041_T-CVD\13_ProgramUsedFile\*.txt" "041_T-CVD\"
copy "T:\04_プロセス関係\10_共通\91_KAIZEN-TDS\01_開発\042_PIX-DML\13_ProgramUsedFile\*.txt" "042_PIX-DML\"

pause