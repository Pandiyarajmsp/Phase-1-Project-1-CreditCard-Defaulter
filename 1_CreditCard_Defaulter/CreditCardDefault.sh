
Datetimelog=$(date +"%Y%m%d%H%M%S")
Datetime=$(date +"%Y-%m-%d %H:%M:%S")
StartDatetime=$(date +"%Y-%m-%d %H:%M:%S")

echo "#-----------------------------Function to handle Error entry in log files--------------------------------------------"
Error_Writer() {
		if [ $1 -ne 0 ] && [ "$4" != "file" ] ;
		then
			cat "$2" >> "$3"
			echo "Error in automation flow ,Please check log for more info"
			exit 1
		fi
		if [ $1 -ne 0 ] && [ "$4" = "file" ] ;
		then
			echo $(date +"%y/%m/%d %H:%M:%S")" Error in ""$5" >> "$3"
			echo "Error in automation flow ,Please check log for more info"
			exit 1
		fi
 };



echo "#------------------------------Error and log file direcorty creation---------------------------------------------------"
mkdir -p /home/hduser/1_CreditCard_Defaulter/LogDir/

Logfilename="/home/hduser/1_CreditCard_Defaulter/LogDir/creditcard_insurance.log"
Errorfilename="/home/hduser/1_CreditCard_Defaulter/LogDir/RetailAnalyticsError.log"
echo "" >$Errorfilename


echo "------------------ creditcard_insurance project data load started Good Luck ------------------------------------------"
echo $(date +"%Y/%m/%d %H:%M:%S")" creditcard_insurance project data load started Good Luck " >>"$Logfilename"



 
hadoop fs -mkdir -p /user/hduser/CreditCard_Defaulter_project/ & >>"$Logfilename" 
echo $(date +"%Y/%m/%d %H:%M:%S")" Linux : Directory Creation completed " >>"$Logfilename"

echo "#---------------------Sqoop Import Pre QA check cst -------------------------------------------------------------"
echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import evaluation started for credits_cst table from Mysql" >>"$Logfilename"
Table_name="credits_cst"
sqoop eval --connect jdbc:mysql://inceptez/custdb --username root --password root --query "SELECT count(1) CNT FROM $Table_name " | { tr -cd '[:digit:]' ; echo ; } >/home/hduser/1_CreditCard_Defaulter/LogDir/cst_count_data.txt
cst_source_Count=$(cat /home/hduser/1_CreditCard_Defaulter/LogDir/cst_count_data.txt) 

echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import evaluation completed for credits_cst table from Mysql and found the $cst_source_Count records in source " >>"$Logfilename"

echo "#----------------------Sqoop Import Started for cst -----------------------------------------------------------------"
if [ $cst_source_Count -ne 0 ] ; then  
	echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : started for credits_cst table from Mysql  " >>"$Logfilename"
	sqoop import --connect jdbc:mysql://inceptez/custdb --username root --password root --table credits_cst --delete-target-dir --target-dir /user/hduser/CreditCard_Defaulter_project/credits_cst/ --split-by id --m 3  
	echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import completed successfully" >>"$Logfilename"
	else 
	echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import Source table $Table_name is empty , please check with source team or try later" >>"$Logfilename"
	exit 1
fi

echo "#---------------------Sqoop Import post QA check cst -------------------------------------------------------------"
echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import  source and destination Post QA started for credits_cst " >>"$Logfilename"
cst_Dest_Count=$(hadoop fs -cat /user/hduser/CreditCard_Defaulter_project/credits_cst/* | wc -l) 
if [ $cst_Dest_Count -eq $cst_source_Count ] ; then
	 echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import source $cst_source_Count and destination $cst_Dest_Count counts are matched  " >>"$Logfilename"
	 echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import source $cst_source_Count and destination $cst_Dest_Count counts are matched  "
	 else
	 echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import source $cst_source_Count and destination $cst_Dest_Count counts are not matched  "
	 echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import source $cst_source_Count and destination $cst_Dest_Count counts are not matched  " >>"$Logfilename"
	 exit 1
	
fi


echo "#----------------------Sqoop Import Started for pst -----------------------------------------------------------------"

echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import evaluation started for credits_pst table from Mysql" >>"$Logfilename"

Table_name="credits_pst"
sqoop eval --connect jdbc:mysql://inceptez/custdb --username root --password root --query "SELECT count(1) CNT FROM $Table_name " | { tr -cd '[:digit:]' ; echo ; } >/home/hduser/1_CreditCard_Defaulter/LogDir/pst_count_data.txt

pst_source_Count=$(cat /home/hduser/1_CreditCard_Defaulter/LogDir/pst_count_data.txt) 
echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import evaluation completed for credits_pst table from Mysql and found the $pst_source_Count records in source " >>"$Logfilename"

if [ $pst_source_Count -ne 0 ] ; then  
	echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import  started for credits_pst table from Mysql  " >>"$Logfilename"
	sqoop import --connect jdbc:mysql://inceptez/custdb --username root --password root --table credits_pst --delete-target-dir --target-dir /user/hduser/CreditCard_Defaulter_project/credits_pst/ --split-by id --m 3  
	echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import completed successfully" >>"$Logfilename"
	else 
	echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import Source table $Table_name is empty , please check with source team or try later" >>"$Logfilename"
	exit 1

fi
echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import  source and destination post QA check started for credits_pst " >>"$Logfilename"
pst_Dest_Count=$(hadoop fs -cat /user/hduser/CreditCard_Defaulter_project/credits_pst/* | wc -l) 
if [ $pst_Dest_Count -eq $pst_source_Count ] ; then
 	 echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import source $pst_source_Count and destination $pst_Dest_Count counts are matched  " >>"$Logfilename"
	 else
	 echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : import source $pst_source_Count and destination $pst_Dest_Count counts are not matched  " >>"$Logfilename"
	 exit 1
fi

echo "#----------------------Sqoop Import completed for pst -----------------------------------------------------------------"

hadoop fs -rm -r -f /user/hduser/CreditCard_Defaulter_project/defaultersout/ 
hadoop fs -rm -r -f /user/hduser/CreditCard_Defaulter_project/nondefaultersout/ 

echo "#-------------Temp table creation and loading merged data into cstpstreorder where billamt >0 " 
echo $(date +"%Y/%m/%d %H:%M:%S")" Hive  : ELT -Custdb Temp table creation and loading merged data into cstpstreorder where billamt >0 started " >>"$Logfilename"
hive -f "/home/hduser/1_CreditCard_Defaulter/Credit_cstpstreorder_load.hql"
Error_Writer $? "$Errorfilename" "$Logfilename" "file" "Credit_cstpstreorder_load.hql"
echo $(date +"%Y/%m/%d %H:%M:%S")" Hive  : ELT Temp table creation and loading merged data into cstpstreorder where billamt >0 completed " >>"$Logfilename"

echo "##-------------Insure and State master table creation and data load from csv and fixed width file and create defaulter internal table "
echo $(date +"%Y/%m/%d %H:%M:%S")" Hive  : ELT -Insuredb Insure and State master table creation and data load from csv and fixed width file and create defaulter internal table started " >>"$Logfilename"
hive -f "/home/hduser/1_CreditCard_Defaulter/Credit_Insure_load.hql" 
Error_Writer $? "$Errorfilename" "$Logfilename" "file" "Credit_Insure_load.hql"
echo $(date +"%Y/%m/%d %H:%M:%S")" Hive  : ELT -Insuredb Insure and State master table creation and data load from csv and fixed width file and create defaulter internal table completed " >>"$Logfilename"




echo "#----------------insuranceorc internale table creation and data load started --------------"


echo $(date +"%Y/%m/%d %H:%M:%S")" Hive  : ELT -Insuredb insuranceorc internale table creation started " >>"$Logfilename"
hive -f "/home/hduser/1_CreditCard_Defaulter/Credit_insuranceorc_TableCreate.hql"
Error_Writer $? "$Errorfilename" "$Logfilename" "file" "Credit_insuranceorc_TableCreate.hql"
echo $(date +"%Y/%m/%d %H:%M:%S")" Hive  : ELT -Insuredb insuranceorc internale table creation completed " >>"$Logfilename"

#hadoop  fs -rm -r -f /user/hduser/CreditCard_Defaulter_project/insuranceorc/ 


echo $(date +"%Y/%m/%d %H:%M:%S")" Hive  : ELT -Insuredb insuranceorc internale table and middlegradeview creation started " >>"$Logfilename"
hive -f "/home/hduser/1_CreditCard_Defaulter/Credit_insuranceorc_load.hql" 
Error_Writer $? "$Errorfilename" "$Logfilename" "file" "Credit_insuranceorc_load.hql"
echo $(date +"%Y/%m/%d %H:%M:%S")" Hive  : ELT -Insuredb insuranceorc internale table and middlegradeview creation completed " >>"$Logfilename"
echo "#----------------insuranceorc internale table creation and data load completed --------------"

echo "#-----------Sqoop Export started -----------------------"
echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : Export - started " >>"$Logfilename"
bash /home/hduser/1_CreditCard_Defaulter/CreditSqoopExport.sh
Error_Writer $? "$Errorfilename" "$Logfilename" "file" "CreditSqoopExport.sh"
echo $(date +"%Y/%m/%d %H:%M:%S")" Sqoop : Export - completed " >>"$Logfilename"
echo "#-----------Sqoop Export completed -----------------------"










