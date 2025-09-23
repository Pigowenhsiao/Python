@echo off

if not "%1" == "1" (
    start /min cmd /c call "%~f0" 1
    exit
)

cd /d %~dp0

cd "../001_GRATING/"
python CVD_Crystal_Length.py
python CVD_Mask_Length.py

cd "../002_MESA/"
python MESA_CVD_Width.py

cd "../003_N-electrode/"
python Polish_main.py
python Polish_main_Grinder.py

cd "../004_T2-EML/"
python T2-EML_F2.py
python T2-EML_F6.py

cd "../005_BJ1/"
python BJ1_CVD_EtchingRate.py

cd "../006_BJ1/"
python BJ1_Crystal_Depth.py

cd "../007_BJ2/"
python BJ2_Crystal_Depth.py

cd "../008_WG-EML/F1/"
python WG_EML_F1.py
cd "../F2/"
python WG_EML_F2.py
cd "../F6/"
python WG_EML_F6.py
cd "../F7/"
python WG_EML_F7.py

cd "../../009_GRATING/"
python GRATING_EB-Duty.py

cd "../010_GRATING/"
python GRATING_CVD_EtchingRate.py

cd "../011_MESA/"
python MESA_EB_Width.py

cd "../012_MESA/"
python MESA_CVD_EtchingRate.py

cd "../013_MESA/"
python MESA_Crystal_Depth_Dry.py
python MESA_Crystal_Depth_ICP.py

cd "../014_PIX/"
python PIX.py

cd "../015_P-electrode/"
python P-electrode_InP.py
python P-electrode_SiN.py

cd "../016_P-electrode/"
python P-electrode.py

cd "../017_GRATING/"
python DML-EB-Duty.py

cd "../018_T2-DML/F5-10G/"
python T2-DML_F5-10G.py
cd "../F5-25G/"
python T2-DML_F5-25G.py
cd "../F6-10G/"
python T2-DML_F6-10G.py
cd "../F6-25G/"
python T2-DML_F6-25G.py

cd "../../019_MESA/"
python BNKPhoto.py

cd "../020_MESA/"
python MESA_CVD_EtchingRate_DML.py

cd "../021_MESA/"
python MESA_CAP_Depth.py

cd "../022_P-electrode/"
python P-electrode.py

cd "../023_P-electrode/"
python P-electrode.py

cd "../024_ISO-EML/"
python ISO-EML.py

cd "../025_PIX/"
python PIX.py

cd "../026_PIX/"
python PIX.py

cd "../027_MESA/"
python MESA_Wet_Depth.py

cd "../028_TH-DML/"
python TH-DML.py

cd "../029_TH-DML/"
python TH-DML_SiO2_EtchingRate.py

cd "../030_N-electrode/"
python N-electrode_N-ISO_Width.py

cd "../031_SEM-EML/"
python SEM-EML_main.py

cd "../032_SEM-DML/"
python SEM-DML_main.py

cd "../033_N-electrode/"
python N-electrode.py

cd "../034_N-electrode/"
python N-electrode.py

cd "../035_N-electrode/"
python Pattern_Width.py
python PhotoPattern_Eaves.py
python PhotoPattern_Width.py

cd "../036_N-electrode/"
python Pattern_Width.py
python PhotoPattern_Eaves.py
python PhotoPattern_Width.py

cd "../037_T1-DML/"
python T1-DML.py

cd "../038_EA-EML/F1/"
python EA-EML_F1_Format1.py
python EA-EML_F1_Format2.py
cd "../F7/"
python EA-EML_F7_Format1.py
python EA-EML_F7_Format2.py

cd "../../039_Ru-EML/"
python F3_Main.py
python F4_Main.py

cd "../040_LD-EML/F1/"
python LD-EML_F1_Format1.py
python LD-EML_F1_Format2.py
cd "../F2/"
python LD-EML_F2_Format1.py
python LD-EML_F2_Format2.py
cd "../F6/"
python LD-EML_F6_Format1.py
python LD-EML_F6_Format2.py

cd "../../041_T-CVD/"
python Format1.py
python Format2.py

cd "../042_PIX-DML/"
python PIX_DML.py

python ../SQL_Program/SQL_Program.py

cd "../OtherPrograms/Graph_Program/"
python Graph_Create.py

cd "../LogCheckProgram/"
python Write_MariaDB.py

pause