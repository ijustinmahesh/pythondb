#!/opt/collabnet/teamforge/runtime/bin/wrapper/python
import DBapp.util
import DBapp.logutil
import DBapp.db
import psycopg2
#import psycopg2.extras
import time
import datetime
import os
import sys
import psycopg2.extras

def create_entry(logger,DBConn)            :

   try    :
       ValidatePrePostrun  =""" select count(*) as cnt from migration_log where id='Datacomparator' ; """
       InsertQMigrationLog =""" insert into migration_log 
                                values('Datacomparator',now(),'Pre','Post','DataReconcillation.py','Migation data comparator')  """
     
       DBConn.execute(ValidatePrePostrun)
       ValidatePrePostData=DBConn.fetchall()
       if ValidatePrePostData[0][0] ==1:
          PostRunFlg ='on'
          printMessage('considered as post migration run')
       else                           : 
          PostRunFlg ='off' 
          printMessage('considered as pre migration run')

          DBConn.execute(InsertQMigrationLog)

       return PostRunFlg
   except :
       print "Data comparator failed please check in  runtime.log for details."

def create_DBObjects(logger,DBConn)        :
   try    :
       DropTableQ          ='Drop table if exists datacomparator'
       CreateTableQ        =""" create table if not exists datacomparator (table_name text ,premig_cnt int,pstmig_cnt int ,pre_md5 text,post_md5 text ,change_new_flg char(1)) ; """
       DBConn.execute(DropTableQ)
       DBConn.execute(CreateTableQ)
       CreateRowcntFn      =""" CREATE OR REPLACE  FUNCTION getrowcount(v_table_name text) RETURNS int AS $$ 
                                                 DECLARE
                                          		sqlstmnt       text   ;
                                                        v_row_cnt      int    ;
                                                  BEGIN
                                                        BEGIN  
                                              		       sqlstmnt = 'SELECT count(*)  FROM '|| v_table_name;
                                  		               EXECUTE sqlstmnt INTO v_row_cnt  ;
                                              	        EXCEPTION
                                                 	       WHEN OTHERS THEN
                                              		            raise EXCEPTION  'Value: %', v_table_name;   
                                                        END ;
                                                        RETURN v_row_cnt		;
                                                   END; 
                                $$ LANGUAGE PLPGSQL; """
       DBConn.execute(CreateRowcntFn)
       CreateTableMD5Fn      =""" CREATE OR REPLACE  FUNCTION gettablemd5(v_table_name text) RETURNS Text AS $$ 
                                                 DECLARE
                                                        sqlstmnt       text   ;
                                                        v_row_md5      text    ;
                                                  BEGIN
                                                        BEGIN  
                                                               sqlstmnt = 'SELECT  md5(array_to_string(array_agg(md5('||v_table_name||'::text)),'''')) from '||v_table_name;
                                                               EXECUTE sqlstmnt INTO v_row_md5  ;
                                                        EXCEPTION
                                                               WHEN OTHERS THEN
                                                                    raise EXCEPTION  'Value: %', sqlstmnt;   
                                                        END ;
                                                        RETURN v_row_md5                ;
                                                   END; 
                                $$ LANGUAGE PLPGSQL;  """
       DBConn.execute(CreateTableMD5Fn)
       DBConn.execute('create unique index if not exists  idx_table_name on datacomparator(table_name)')
       CreateTableQ        =""" create table if not exists datarowcomparator (table_name text ,key text,pre_md5 text,post_md5 text ) ; """
       DBConn.execute(CreateTableQ)
       CreateTableMD5Fn    =""" CREATE OR REPLACE  FUNCTION getrowmd5(v_table_name text) RETURNS Text AS $$ 
                                                 DECLARE
                                                        sqlstmnt       text   ;
                                                        v_row_md5      text    ;
                                                  BEGIN
                                                        BEGIN  
                                                               sqlstmnt = 'SELECT  md5(array_to_string(array_agg(md5('||v_table_name||'::text)),'''')) from '||v_table_name;
                                                               EXECUTE sqlstmnt INTO v_row_md5  ;
                                                        EXCEPTION
                                                               WHEN OTHERS THEN
                                                                    raise EXCEPTION  'Value: %', sqlstmnt;   
                                                        END ;
                                                        RETURN v_row_md5                ;
                                                   END; 
                                $$ LANGUAGE PLPGSQL;  """




       printMessage("creation of table/function   completed")
       CreateTblSQL         =""" """

   except :
       print "Data comparator failed please check in  runtime.log for details."
       logger.error( "create db components failed for %s" )


def tablecountmd5   (logger,DBConn,PostRunFlg)  :
   try    :
       RowCntmd5Sql  ="""select table_name,getrowcount(table_name),getrowmd5(table_name) 
                       from information_schema.tables 
                       where table_schema ='public' order by 1"""
       if PostRunFlg == 'on' :
          UpsertSql=""" insert  into datacomparator as dc(table_name,pstmig_cnt,post_md5) 
                       select  table_name           AS table_name, 
                               getrowcount(table_name) AS rc, 
                               gettablemd5(table_name)   AS rmd5 
                       from    information_schema.tables 
                       where   table_schema ='public'  
                       on conflict (table_name)
                       do update 
                       SET    pstmig_cnt =excluded.pstmig_cnt ,
                              post_md5   =excluded.post_md5  """
          DBConn.execute(UpsertSql)
          printMessage("Row count and MD5 generation on table level for  post migration  completed")
       else :
          InsertSql  ="""insert into datacomparator(table_name,premig_cnt,pre_md5) 
                       select table_name,getrowcount(table_name),gettablemd5(table_name) 
                       from information_schema.tables 
                       where table_schema ='public' order by 1"""
          DBConn.execute(InsertSql)
          printMessage("Row count and MD5 generation on table level for  pre migration  completed")
       return True
   except :
       print "Data comparator failed please check in  runtime.log for details."
       logger.error( "Poulate row count is failed ")



def reconcillation(logger,DBConn)  :
   try    :
      
       DataCompareSql  =""" copy(select table_name as TableName,premig_cnt PreMigrationCount,pstmig_cnt PostMigrationCount,pre_md5 PreMigrationTableMD5,
                                 post_md5 as PostMigrationTableMD5   from datacomparator
                        where ( (coalesce(premig_cnt,null,0) <> coalesce(pstmig_cnt,null,0)) or (pre_md5<>post_md5) ) and table_name not in ('datacomparator') ) TO STDOUT WITH CSV HEADER"""
       ReportHdr=['*** Data verification report  ***']
       ColumnHdr=['TableName','PreMigCnt','PstMigCnt','PreMD5','PostMD5']
       ReportFtr=['***  End of data verification report  ***']
       FileName='/tmp/PrePostMigDataComparision.csv'
       with open(FileName,'w') as filenameop:
            DBConn.copy_expert(DataCompareSql,filenameop)
            printMessage("Reconcillation completed")
            logger.info( "Reconcillation complated" )
       printMessage("Refer /tmp/PrePostMigDataComparision.csv for comparision report ")
       return True

   except :
       print "Data comparator failed please check in  runtime.log for details."
       logger.info( " Failed in Data reconcillation ")


def cleanup(logger,DBConn)                             :                    
   try    :
      CleanUpSql =""" delete from  migration_log  where id ='Datacomparator'  """
      DBConn.execute(CleanUpSql)
      printMessage("cleanup  process completed")
      logger.info( "Comparision process  completed")

   except :
       print "Export Failed refer runtime.log for details."
       logger.info( " Failed in cleanup")




def main():
   try:
       cfg 		    =app.util.getRuntimeConfiguration()
       logger               =app.logutil.getRuntimeLogger()
       appHost              =cfg.get('appcore-database-admin/dependency/appcore-database/jdbc/host')
       appDBPort	    =cfg.get('appcore-database-admin/dependency/appcore-database/jdbc/port')
       appDBUser            =cfg.get('DATABASE_USERNAME')
       appDB                =cfg.get('DATABASE_NAME')
       appDBPWD             =cfg.get('DATABASE_PASSWORD')
#       conn                 =psycopg2.connect(user = appDBUser,password = appDBPWD,database = appDB,host = appHost, port = appDBPort)
#       conn.autocommit =True
#       app                  =conn.cursor()
       with psycopg2.connect(user = appDBUser,password = appDBPWD,database = appDB,host = appHost, port = appDBPort) as conn:
         conn.autocommit =True
         app             =conn.cursor()
         MigFlg               =create_entry(logger,app)
         if MigFlg =='off'     :
            create_DBObjects(logger,app)   
         CheckProcessStatus=tablecountmd5(logger,app,MigFlg)  
         if CheckProcessStatus==True and MigFlg=='on':
            CheckProcessStatus=reconcillation(logger,app)
            if CheckProcessStatus==True:
               cleanup(logger,app)
             
       conn.close()
       printMessage("process  completed")
       logger.info( "process  completed")

   except :
       print "core comparision failed, refer runtime.log for details."
       logger.info( " Failed in core process")



def printMessage(message):
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    print "[" + st + "]: " + message
    sys.stdout.flush()

if __name__== "__main__" :
  main() 