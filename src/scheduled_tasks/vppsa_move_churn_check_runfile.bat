call "C:\ProgramData\Anaconda3\Scripts\activate.bat"
call pushd "\\glawi222\data$\Data\Common\New Energy\Commercial Development\Data strategy and analytics\12 vppsa-move-and-churn-check\vppsa_move_churn_check\"
call activate myenv_test1
call python -m src.code.vppsa_move_churn_check "jjazaeri@agl.com.au" "P:/New Energy/Churn Moveout Report/Input_file/Full VPPSA Site List V3.xlsx" "P:/New Energy/Churn Moveout Report/Output_file" "TKelly-Spanner@agl.com.au"
call popd
pause
