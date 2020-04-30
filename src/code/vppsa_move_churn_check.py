import sys



from sqlalchemy import create_engine, types
import pandas as pd
import time as time
from datetime import datetime
import mailer
import getpass
    
def check_email(user,sender_email, path_input,path_output, reciever_emails):


    t_preliminary_0 = time.time()
    # Set user.
    # Create engine.
    engine = create_engine('hana://{user}@hananode1:30015'.format(user=user))
    
    
    df1 =pd.read_excel(path_input)
    
    df1.to_sql('vpp_churn_tom_from_python', engine, schema=user, if_exists='replace', dtype=types.NVARCHAR(length=255))
    
    t_preliminary_1 =  time.time()
    
    # Wait for 5 seconds
    #time.sleep(300)
    
    t_sql_code_0 = time.time()
    sql="""
    SELECT A."Inverter", A."POD (NMI)" NMI, A."*approved BP*" BP_VPP, C.BUSINESSPARTNER BP_Active,C.COMPANY,
    min(CASE WHEN C.BUSINESSPARTNER IS NULL THEN '3_LeftVPP_New_NonAGL_Customer' 
    WHEN C.BUSINESSPARTNER IS NOT NULL AND right(A."*approved BP*",9) <> right(C.BUSINESSPARTNER,9) and C.COMPANY != 'AGL' THEN '3_LeftVPP_New_NonAGL_Customer'
    WHEN C.BUSINESSPARTNER IS NOT NULL AND right(A."*approved BP*",9) <> right(C.BUSINESSPARTNER,9) THEN '4_LeftVPP_New_AGL_Customer'
    when C.BUSINESSPARTNER IS NOT NULL AND right(A."*approved BP*",9) = right(C.BUSINESSPARTNER,9) and C.COMPANY = 'PD' THEN '2_PowerDirect'
    ELSE '1_CURRENT' END) AS STATUS
    , CASE WHEN A."*approved BP*" IS NOT NULL THEN (SELECT max(MOVEINDATE) from "SP_CUSTOMER"."CIA_TheTruthAboutCustomer"D where right(D.BUSINESSPARTNER,9) = right(A."*approved BP*",9) and left(D.NMI,10)=left(A."POD (NMI)",10)) END VPP_MOVEIN
    , CASE WHEN A."*approved BP*" IS NOT NULL THEN (SELECT max(MOVEOUTDATE) from "SP_CUSTOMER"."CIA_TheTruthAboutCustomer"D where right(D.BUSINESSPARTNER,9) = right(A."*approved BP*",9) and left(D.NMI,10)=left(A."POD (NMI)",10)) END VPP_MOVEOUT
    ,CASE WHEN C.BUSINESSPARTNER IS NOT NULL THEN (SELECT max(MOVEINDATE) from "SP_CUSTOMER"."CIA_TheTruthAboutCustomer"D where right(D.BUSINESSPARTNER,9) = right(C.BUSINESSPARTNER,9)and left(D.NMI,10)=left(C.NMI,10)) END CURRENT_CUSTOMER_MOVEIN
    
    from
    	(SELECT * from "{user}"."VPP_CHURN_TOM_FROM_PYTHON") A
    
    left join
    
    	(SELECT * FROM "SP_CUSTOMER"."CIA_TheTruthAboutCustomer" B
    	WHERE FUEL = 'ELEC' AND STATUS = 'ACTIVE'
    	) C on left(A."POD (NMI)",10) = left(C.NMI,10)
    
    GROUP BY A."Inverter", A."POD (NMI)", A."*approved BP*", C.NMI, C.BUSINESSPARTNER, C.TYPE, C.STATE, C.STATUS, C.COMPANY
    order by STATUS
        """.format(user=user)
    
    df2 = pd.read_sql(sql, engine)
    t_sql_code_1 = time.time()
    
    t_exportfile_code_0 = time.time()
    today = datetime.today().date()   
    
    path_output_file = path_output + "/Full VPPSA Site List V4 outputfile {datetime}.xlsx" .format(datetime=today)    
          
    df2.to_excel(path_output_file)
    t_exportfile_code_1 = time.time()
    
    category_all= df2['nmi'].nunique()
    category_1 = df2.groupby('status')['nmi'].nunique()['1_CURRENT']
    category_2 = df2.groupby('status')['nmi'].nunique()['2_PowerDirect']
    category_3 = df2.groupby('status')['nmi'].nunique()['3_LeftVPP_New_NonAGL_Customer']
    category_4 = df2.groupby('status')['nmi'].nunique()['4_LeftVPP_New_AGL_Customer']
    
    ##log
    f= open("P:/New Energy/Churn Moveout Report/LOG_RUN.txt", "a+")
    f.write("%s, %s, %s, %s, %s\n"%(time.strftime("%x, %X"), len(df2), t_preliminary_1-t_preliminary_0,t_sql_code_1-t_sql_code_0,t_exportfile_code_1-t_exportfile_code_0))
    f.close()
    


    if category_2+category_3+category_4>0:
        
        
        
        message = mailer.Message()
        
        message.From = sender_email
        message.To = [reciever_emails]
        message.Subject = 'VPPSA move and Churn Report on {datetime}'.format(datetime=today)
        message.Body = '''Hi all,
            
            On {today_date}, from {category_all_num} unique NMIs in the VPP list, {category_2_num} NMIs are identified as 2_PowerDirect, {category_3_num} NMIs are identified as 3_VPPChurn_New_NonAGL_Customer , {category_4_num} NMIs are identified as 4_VPPChurn_New_AGL_Customer, and %s NMIs are identified as 1_Current. 
            The report is attached to this email and can be find at {path_output_file_loc}.
            
            Definition of Flags:
            1_CURRENT: The Business partner ID in the VPPSA list is the same as the current active Business partner ID at that NMI.
            2_PowerDirect: The Business partner ID in the VPPSA list is the same as the current active Business partner ID at that NMI, but their COMPANY is power direct. 
            3_LeftVPP_New_NonAGL_Customer: The Business partner ID in the VPPSA list  has left that NMI and the new occupant at that NMI is not an AGL customer.
            4_LeftVPP_New_AGL_Customer: The Business partner ID in the VPPSA list  has left that NMI, but the new occupant at that NMI is still an AGL customer.
        
       
            If you have any questions please let me know.
            
            Kind regards,
            
            Javad Jazaeri'''.format(today_date = today, category_all_num = category_all, category_2_num = category_2, category_3_num = category_3, category_4_num =category_4, category_1 = category_1, path_output_file_loc = path_output_file)
    
        #message.attach(path_output_file)
        
        sender = mailer.Mailer('aglsmtp05.agl.com.au')
        
        sender.send(message)
    
    return()

if __name__ == '__main__':
    user = getpass.getuser()
    sender_email = sys.argv[1]
    path_input = sys.argv[2]
    path_output = sys.argv[3]
    reciever_emails = sys.argv[4]
    check_email(user=user,sender_email=sender_email, path_input=path_input,path_output=path_output, reciever_emails=reciever_emails)
