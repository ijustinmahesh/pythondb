#!/opt/net/apt/runtime/bin/wrapper/python

import DBapp.util
import DBapp.logutil
import DBapp.db
import psycopg2
import psycopg2.extras
import time
import datetime
import sys
import getopt
import os
import csv

def main():

   scriptDescription = """ assignment Export:Generates csv file for each mission and csv shall be suitable to do mass import in Tracker.
                           If a mission has more than 4500 assignments then files will be generated in multiples of 4500   .
                           Refer help document for importing exported assignment CSV file to do  mass import . """
   cfg               =DBapp.util.getRuntimeConfiguration()
   DBType            =cfg.get('DATABASE_TYPE')
   if DBType=='postgresql' :
      assignmentPostgres()
   else                     :
      assignmentOracle()

def assignmentPostgres():

   try:


       cfg 	    =app.util.getRuntimeConfiguration()
       logger       =app.logutil.getRuntimeLogger()
       appHost      =cfg.get('host')
       appDBPort    =cfg.get('port')
       appDBUser    =cfg.get('DATABASE-USERNAME')
       appDB        =cfg.get('DATABASE-NAME')
       appDBPWD     =cfg.get('DATABASE-PASSWORD')
       Countsql     ="""      select count(*) ,min(cast(substring(T.ID,5) as int)) ,max(cast(substring(T.id,5) as int)) from assignmentmgr_assignment T inner join article I on (I.id=T.id) inner join  
                                     folder F on (I.folder_id=F.id) Where F.mission_id='%s' and i.is_deleted='f' """
       conn         =psycopg2.connect(user = appDBUser,password = appDBPWD,database = appDB,host = appHost, port = appDBPort)
       app          =conn.cursor()
       _handleParams()
       GetassignmentmissionlistSQL="""Select distinct F.mission_id as  "mission ID" 
                                from assignmentmgr_assignment T 
                                inner join article   I  on (I.id=T.id)
                                inner join folder F  on (I.folder_id=F.id)  
                                where i.is_deleted='f' """
       app.execute(GetassignmentmissionlistSQL)
       missionlist          =app.fetchall()
       printMessage("Intializing assignment data  export ")
       logger.info( "Intializing assignment data  export")

       for mission in missionlist:
                 # process each Active missions
                 missionid=mission[0]
                 GetRowCntSQL=Countsql%(missionid)
                 app.execute(GetRowCntSQL)
                 assignmentData    =app.fetchall()
                 assignmentRowCnt  =int(assignmentData[0][0])
                 printMessage("Exporting assignment data for the mission "+ missionid)
                 logger.info( "Exporting assignment data for the mission "+ missionid)
                 if assignmentRowCnt < int(MaxRows) :
                    #This block only for a mission which has less than 4500 assignments which is  in line with mass import numbers.
                    #This sql pulls assignment  association,assignment dependency,assignment folder hierarchy and assignment attributes
		    COPYSQL              ="""COPY (select T.id                  as "assignment Id"          ,T.priority                         as "Priority"       ,
                                            I.title                             as "Title"            ,       
       CASE 
         WHEN (T.description) is not null and length(trim(T.description)) > 0  THEN  T.description 
         ELSE I.title 
       END                                                                     as "Description"    ,
                                            case when assingedTo='nobody' then null else assingedTo end                                   as "Assigned To"    ,
                                            T.status                            as "Status"                                                                   ,
                                            to_char(T.start_date,'MM/DD/YYYY')  as "Start Date"       ,to_char(T.end_date ,'MM/DD/YYYY')  as "End Date"       ,
                                            T.percent_complete                  as "Percent Complete" ,T.estimated_hours                  as "Estimated Hours",
                                            to_char(T.estimation_start,'MM/DD/YYYY HH:MI:SS')                                             as "Created Date"   ,
                                            u.full_name                         as "assignment Created By"                                                               ,
                                            T.planned                           as "Planned"          ,T.accomplishments                  as "Accomplishments",
                                            T.issues                            as "Issues"           ,T.required_hours                   as "Actual Hours"   ,
                                            case when T.include_weekends='t' then '7 Day(Include weekends)' else '5 Day' end              as "assignment Calendar"  ,
                                            F1.path                             as "Folder Hierarchy" ,DepChildern                        as "assignment Successor" ,
                                            ParentDependency                    as "assignment Predecessor" ,association                        as "assignment Associations"
                                     from  assignmentmgr_assignment T 
                                     inner join article I on (I.id=T.id) 
                                     inner join folder F on(F.id=I.folder_id)
                                     inner join user_info u on (u.id=i.created_by_id)
                                     left  join (select origin_id,string_agg(target_id,',') as DepChildern 
                                                 from liaison
                                                 where liaison_type_name='assignmentDependency'
                                                 and is_deleted = 'f' 
                                                 group by origin_id) as  depndcy on(depndcy.origin_id=T.id)
                                     left  join (select origin_id,string_agg(target_id,',') as association 
                                                 from  ( select origin_id as origin_id,target_id as target_id 
                                                         from liaison
                                                         where liaison_type_name='Generic' 
                                                         and is_deleted = 'f' 
                                                         union all
                                                         select target_id,origin_id  as association 
                                                         from liaison
                                                         where liaison_type_name='Generic' 
                                                        and is_deleted = 'f' 
                                                         order by 1) as innr group by origin_id)  as  asscn
                                                 on (asscn.origin_id=T.id  )
                                     left  join (select s.full_name  as AssingedTo,Target_id  as Tgtid 
                                                 from liaison r inner join user_info s  on (r.origin_id =s.Id )
                                                 where liaison_type_name = 'assignmentAssignment' 
                                                 and is_deleted='f') as AssigndTo on (AssigndTo.Tgtid=T.id)
                                     left  join (select string_agg(origin_id,',')  as ParentDependency ,target_id as assignmentID 
                                                 from liaison
                                                 where liaison_type_name='assignmentDependency'
                                                 and is_deleted = 'f' group by Target_id) as  Pdep on(Pdep.assignmentID=T.id  )
                                     left  join ( WITH RECURSIVE childdeps AS (
                                                      select  T.id TskId,I.folder_id childfid,  parent_folder_id,f.title,0 as level
                                                      from  assignmentmgr_assignment T inner join article I on (I.id=T.id) 
                                                      inner join folder F                    on (I.folder_id=F.id)
                                                      where F.is_deleted ='n'
                                                      union
                                                      select  TskId,childfid,fp.parent_folder_id,fp.Title,c.level+1
                                                      from    folder Fp
                                                      inner join childdeps c  on c.parent_folder_id =fp.id 
                                                      where   fp.id  like 'assignmentgrp%%' and  fp.is_deleted ='n'
                                                ) select TskId,string_agg(title,' > ' order by level desc) as path
                                                  from  childdeps group by TskID order by 1 ) as F1 on (T.id=F1.TskId)
                                     Where F.mission_id='%s' 
                                     and I.is_deleted='n'  
                                     order by T.estimation_start)  TO STDOUT WITH CSV HEADER """	
                    filename='%s_assignmentExport.csv'%(missionid)
                    filename=os.path.join(Path,filename)
                    ExportmissionSQL=COPYSQL%(missionid)
                    with open(filename,'w') as filenameop:
                         app.copy_expert(ExportmissionSQL,filenameop)
                         logger.info( "Exporting of %s assignments completed in mission %s" %(assignmentRowCnt,missionid))
                         logger.info( " completed Query \n %s" %(ExportmissionSQL))
						 
                 else :
                    # This block generates files if a mission has more than 4500 assignments
                    # Assined Min and Max  keys  help to iterate till we reach Max key i.e last assignment
                    MinKey    =assignmentData[0][1]
                    MaxKey    =assignmentData[0][2]
                    LowerRange=MinKey
                    #For a given range ,This sql pulls info of  assignment  association,assignment dependency,assignment folder hierarchy and assignment attributes
                    COPYSQL   ="""  COPY ( select T.Id as "assignment Id",T.priority as "Priority",I.Title  as "Title" ,

       CASE 
         WHEN (T.description) is not null and  length(trim(T.description)) > 0 THEN T.description
         ELSE I.title 
       END                                                AS "Description",
                                              case when AssingedTo='nobody' then null else AssingedTo end                                           as "Assigned To"     ,
					      T.status                                     as "Status"                                                                   ,
                                              to_char(T.start_date,'MM/DD/YYYY')           as "Start Date"       ,to_char(T.end_date,'MM/DD/YYYY')  as "End Date"        ,
                                              T.percent_complete                           as "Percent Complete" ,T.estimated_hours                 as "Estimated Hours" ,
                                              to_char(T.estimation_start,'MM/DD/YYYY HH:MI:SS')                                                     as "Created Date"    ,
                                              u.full_name                                  as "assignment Created By"                                                               ,
                                              T.planned                                    as "Planned"          ,T.ACCOMPLISHMENTS                 as "Accomplishments" ,
                                              T.issues                                     as "Issues"           ,T.REQUIRED_HOURS                  as "Actual Hours"  ,
                                              F1.path                                      as "Folder Hierarchy"                                                         ,
                                              case when T.include_weekends='t' then '7 Day(Include weekends)' else '5 Day' end                      as "assignment Calendar"   ,
                                              DepChildern                                  as "assignment Successor"   ,ParentDependency                  as "assignment Predecessor",
     				              association                                  as "assignment Associations"
                                     from  assignmentmgr_assignment T inner join article I on (I.id=T.id) inner join folder F on (I.folder_id=F.id) 
                                     inner join user_info u on (u.id=i.created_by_id)
                                     left join  (select origin_id,string_agg(target_id,',') as DepChildern from  liaison 
                                                 where liaison_type_name='assignmentDependency' and   is_deleted = 'f' group by origin_id)           as  associtn 
					         on(associtn.origin_id=T.id)
                                     left join  (select origin_id,string_agg(target_id,',') as association from  (select origin_id as origin_id,target_id as target_id 
                                                 from liaison
                                                 where liaison_type_name='Generic' 
                                                 and is_deleted = 'f' 
                                                 union all  select target_id,origin_id  as association 
                                                 from liaison
                                                 where liaison_type_name='Generic' 
                                                 and is_deleted = 'f' 
                                                  order by 1) as innr group by origin_id)  as asscn  on(asscn.origin_id=T.id  ) 
                                     left join  (select s.full_name as AssingedTo,Target_id  as Tgtid 
                                                 from   liaison r inner  join user_info s  on (r.origin_id =s.Id ) 
                                                 where  liaison_type_name = 'assignmentAssignment' and  is_deleted='f') as AssigndTo
                                                 on(AssigndTo.Tgtid=T.id)         
                                     left join  (select string_agg(origin_id,',')  as ParentDependency ,target_id as assignmentID 
                                                 from liaison
                                                 where liaison_type_name='assignmentDependency'
                                                 and is_deleted = 'f' group by Target_id ) as  Pdep on(Pdep.assignmentID=T.id  )
                                     left join  (WITH RECURSIVE childdeps AS (
                                                      select  T.id TskId,I.folder_id childfid,  parent_folder_id,f.title,0 as level
                                                      from  assignmentmgr_assignment T inner join article I on (I.id=T.id) 
                                                      inner join folder F                    on (I.folder_id=F.id)
                                                      where F.is_deleted ='n'
                                                      union
                                                      select  TskId,childfid,fp.parent_folder_id,fp.Title,c.level+1
                                                      from    folder Fp
                                                      inner join childdeps c  on c.parent_folder_id =fp.id 
                                                      where   fp.id  like 'assignmentgrp%%' and  fp.is_deleted ='n'
                                                ) select TskId,string_agg(title,' > ' order by level desc) as path
                                                  from  childdeps group by TskID order by 1 ) as F1 on (T.id=F1.TskId)
                                                      Where   F.mission_id='%s' 
                                                      and cast(substring(T.id,5) as int) between (%s) and (%s)  
						      order by T.ESTIMATION_START)  TO STDOUT WITH CSV HEADER """
                    printMessage("Exporting assignment data for %s"%((missionid)))
                    while int(MinKey)     <= int(MaxKey) :
                          UpperRange      =int(MinKey)+int(MaxRows)
                          ExportmissionSQL=  COPYSQL%(missionid,MinKey,UpperRange)                          
                          filename="%s_assignmentExportPart%s.csv"%(missionid,MinKey)
                          filename=os.path.join(Path,filename)
                          MinKey  =int(UpperRange)+1

                          with open(filename,'w') as filenameop:
                               logger.info("***Key range from %s to %s *** "%(MinKey,UpperRange) )
                               app.copy_expert(ExportmissionSQL,filenameop)
                               MinKey  =int(UpperRange)+1
                               logger.info( " completed Query \n %s" %(ExportmissionSQL))                    
       app.close()
       printMessage("Exporting assignment data completed")
       logger.info( "Exporting assignment data completed")
   except :
       print "Please refer runtime.log for any failures."
       logger.error( "Exporting assignment data failed " )

def assignmentOracle()         :

   try:
       scriptDescription    ="""assignment Export:Generates csv file for each mission and csv shall be suitable to do mass import in Tracker.
                            If a mission has more than 4500 assignments then files will be generated in multiples of 4500   .
                            Refer help document for importing exported assignment CSV file to do  mass import . """
       cfg 		    =app.util.getRuntimeConfiguration()
       logger               =app.logutil.getRuntimeLogger()
       appHost              =cfg.get('appcore-database-admin/dependency/appcore-database/jdbc/host')
       appDBPort	    =cfg.get('appcore-database-admin/dependency/appcore-database/jdbc/port')
       appDBUser            =cfg.get('DATABASE_USERNAME')
       appDB                =cfg.get('DATABASE_NAME')
       appDBPWD             =cfg.get('DATABASE_PASSWORD')
       _handleParams()
       CSVHeader            =['assignment Id'     ,'Priority','Title', 'Description',
                               'Assigned To','Status',
                               'Start Date' ,'End Date',
                               'Percent Complete','Estimated Hours',
                               'assignment Created By' ,'Created Date',
                               'Planned'    ,'Accomplishments','Issues',
                               'Actual Hours',
                               'assignment Calendar',
                               'Folder Hierarchy','assignment Successor','assignment Predecessor','assignment Associations']
       Countsql             ="""select count(*),min(to_number(substr(T.ID,5))),max(to_number(substr(T.id,5))) from assignmentmgr_assignment T inner join article I on (I.id=T.id) inner join folder F on (I.folder_id=F.id) Where F.mission_id='%s' """
       with app.db.Database.connect('oracle',appHost,appDBPort,appDB,appDBUser,appDBPWD) as app       :
            GetassignmentmissionlistSQL=""" Select distinct F.mission_id as "mission ID"
                                      from assignmentmgr_assignment T inner join article I on (I.id=T.id) 
                                      inner join  folder F on (I.folder_id=F.id) 
                                      where i.is_deleted =0"""
            missionlist         =app.query(GetassignmentmissionlistSQL)
            printMessage("Intializing assignment data  export")
            logger.info( "Intializing assignment data  export")

            for  mission in missionlist:
                 missionid   =mission[0]
                 GetRowCntSQL=Countsql%(missionid)
                 assignmentData    =app.query(GetRowCntSQL)
                 assignmentRowCnt  =int(assignmentData[0][0])
                 printMessage("Exporting assignment data for the mission "+ missionid)
                 logger.info( "Exporting assignment data for the mission "+ missionid)
                 if assignmentRowCnt < int(MaxRows) :
                    filename='%s_assignmentExport.csv'%(missionid)
                    filename=os.path.join(Path,filename)
#                    filename =Path+"%s_assignmentExport.csv"%(missionid)
                    with open(filename,'w') as filenameop:
                         COPYSQL="""SELECT 
       T.id                                               AS "assignment Id", 
       T.priority                                         AS "Priority", 
       I.title                                            AS "Title", 
       CASE 
         WHEN T.description is not null and length(trim(T.description)) > 0 THEN  to_char(T.description) 
         ELSE I.title 
       END                                                AS "Description", 
       CASE 
         WHEN assingedto = 'nobody' THEN NULL 
         ELSE assingedto 
       END                                                AS "Assigned To", 
       T.status                                           AS "Status", 
       To_char(T.start_date, 'MM/DD/YYYY')                AS "Start Date", 
       To_char(T.end_date, 'MM/DD/YYYY')                  AS "End Date", 
       T.percent_complete                                 AS "Percent Complete", 
       T.estimated_hours                                  AS "Estimated Hours", 
       u.full_name                                        AS "Created By", 
       To_char(T.estimation_start, 'MM/DD/YYYY HH:MI:SS') AS "Created Date", 
       T.planned                                          AS "Planned", 
       T.accomplishments                                  AS "Accomplishments", 
       T.issues                                           AS "Issues", 
       T.required_hours                                   AS "Actual Hours", 
       CASE 
         WHEN T.include_weekends = 1 THEN '7 Day(Include weekends)' 
         ELSE '5 Day' 
       END                                                AS "assignment Calendar", 
       F1.path                                            AS "FldrHirchy", 
       depchildern                                        AS "TskDepChild", 
       association                                        AS "TskAsscn", 
       parentdependency                                   AS "assignment Parent" 
FROM   assignmentmgr_assignment T 
       inner join article I 
               ON ( I.id = T.id ) 
       inner join folder F 
               ON ( I.folder_id = F.id ) 
       inner join user_info u 
               ON ( u.id = i.created_by_id ) 
       left join (SELECT origin_id, 
                         Listagg(target_id, ',') 
                           within GROUP (ORDER BY target_id ) AS DepChildern 
                  FROM   liaison 
                  WHERE  liaison_type_name = 'assignmentDependency' 
                         AND is_deleted = 0 
                  GROUP  BY origin_id) childepncy 
              ON( childepncy.origin_id = T.id ) 
       left join (select origin_id, 
                         listagg(target_id, ',') 
                         within group (order by target_id ) AS association
                  from  (select origin_id as origin_id,target_id as target_id 
                         from liaison
                         where liaison_type_name='Generic' 
                         and is_deleted = 0 
                         union all
                         select target_id,origin_id  as association 
                         from liaison
                         where liaison_type_name='Generic' 
                         and is_deleted = 0
                         order by 1) innr group by origin_id		  
                 ) assn 
              ON( assn.origin_id = T.id ) 
       left join (SELECT s.full_name AS AssingedTo, 
                         target_id  AS Tgtid 
                  FROM   liaison r 
                         inner join user_info s 
                                 ON ( r.origin_id = s.id ) 
                  WHERE  liaison_type_name = 'assignmentAssignment' 
                         AND is_deleted = 0) AssigndTo 
              ON ( AssigndTo.tgtid = T.id ) 
       left join (SELECT listagg(origin_id,',') within GROUP (ORDER BY origin_id ) AS ParentDependency, 
                         target_id AS assignmentID 
                  FROM   liaison 
                  WHERE  liaison_type_name = 'assignmentDependency' 
                         AND is_deleted = 0 group by target_id) Pdep 
              ON( Pdep.assignmentid = T.id ) 
       left join (       
WITH childdeps (tid, childfid, parent_folder_id, title, levels1 ) 
                       AS (SELECT t.id as Tid ,I.folder_id childfid, 
                                  parent_folder_id, 
                                  f.title, 
                                  0           levels1 
                           FROM   assignmentmgr_assignment T 
                                  inner join article I 
                                          ON ( I.id = T.id ) 
                                  inner join folder F 
                                          ON ( I.folder_id = F.id ) 
                           WHERE  F.is_deleted = 0 
                           UNION ALL 
                           SELECT  Tid,childfid, 
                                  fp.parent_folder_id, 
                                  fp.title, 
                                  levels1 + 1 AS levels1 
                           FROM   folder Fp 
                                  inner join childdeps c 
                                          ON c.parent_folder_id = fp.id 
                           WHERE  fp.id LIKE 'assignmentgrp%%' and Fp.is_deleted = 0) 
                  SELECT tid, 
                         Listagg(title, ' > ') 
                           within GROUP (ORDER BY levels1 DESC ) AS path 
                   FROM   childdeps 
                   GROUP  BY Tid) F1 
              ON ( T.id = F1.Tid ) 
WHERE  F.mission_id = '%s' 
       AND i.is_deleted = 0"""
                         ExportmissionSQL=COPYSQL%(missionid)
                         DataSet=app.query(ExportmissionSQL)
                         writer =csv.writer(filenameop)
                         writer.writerow(CSVHeader)
                         for row in DataSet:
                             cols = list(row)
                             writer.writerow(cols)
                             logger.info( "Exporting of %s assignments completed in mission %s" %(assignmentRowCnt,missionid))
                             logger.info( " completed Query \n %s" %(ExportmissionSQL))
                 else :
                    NoOfFiles =divmod(int(assignmentRowCnt),int(MaxRows))
                    FileItrn  =NoOfFiles[0]
                    MinKey    =assignmentData[0][1]
                    MaxKey    =assignmentData[0][2]
                    LowerRange=MinKey
                    COPYSQL   ="""SELECT 
       T.id                                               AS "assignment Id", 
       T.priority                                         AS "Priority", 
       I.title                                            AS "Title", 
       CASE 
         WHEN T.description is not null and length (trim(T.description)) > 0 THEN  to_char(T.description) 
         ELSE I.title 
       END                                               AS "Description", 
       CASE 
         WHEN assingedto = 'nobody' THEN NULL 
         ELSE assingedto 
       END                                                AS "Assigned To", 
       T.status                                           AS "Status", 
       To_char(T.start_date, 'MM/DD/YYYY')                AS "Start Date", 
       To_char(T.end_date, 'MM/DD/YYYY')                  AS "End Date", 
       T.percent_complete                                 AS "Percent Complete", 
       T.estimated_hours                                  AS "Estimated Hours", 
       u.full_name                                        AS "Created By", 
       To_char(T.estimation_start, 'MM/DD/YYYY HH:MI:SS') AS "Created Date", 
       T.planned                                          AS "Planned", 
       T.accomplishments                                  AS "Accomplishments", 
       T.issues                                           AS "Issues", 
       T.required_hours                                   AS "Actual Hours", 
       CASE 
         WHEN T.include_weekends = 1 THEN '7 Day(Include weekends)' 
         ELSE '5 Day' 
       END                                                AS "assignment Calendar", 
       F1.path                                            AS "FldrHirchy", 
       depchildern                                        AS "TskDepChild", 
       association                                        AS "TskAsscn", 
       parentdependency                                   AS "assignment Parent" 
FROM   assignmentmgr_assignment T 
       inner join article I 
               ON ( I.id = T.id ) 
       inner join folder F 
               ON ( I.folder_id = F.id ) 
       inner join user_info u 
               ON ( u.id = i.created_by_id ) 
       left join (SELECT origin_id, 
                         Listagg(target_id, ',') 
                           within GROUP (ORDER BY target_id ) AS DepChildern 
                  FROM   liaison 
                  WHERE  liaison_type_name = 'assignmentDependency' 
                         AND is_deleted = 0 
                  GROUP  BY origin_id) childepncy 
              ON( childepncy.origin_id = T.id ) 
       left join (SELECT origin_id, 
                         Listagg(target_id, ',') 
                         within GROUP (ORDER BY target_id ) AS association from
                                                (select origin_id as origin_id,target_id as target_id 
                                                 from liaison
                                                 where liaison_type_name='Generic' 
                                                 and is_deleted = 0 
                                                 union all  select target_id,origin_id  as association 
                                                 from liaison
                                                 where liaison_type_name='Generic' 
                                                 and is_deleted = 0
                                                 order by 1
                                                )  innr group by origin_id
                ) assn 
              ON( assn.origin_id = T.id ) 
       left join (SELECT s.full_name AS AssingedTo, 
                         target_id  AS Tgtid 
                  FROM   liaison r 
                         inner join user_info s 
                                 ON ( r.origin_id = s.id ) 
                  WHERE  liaison_type_name = 'assignmentAssignment' 
                         AND is_deleted = 0) AssigndTo 
              ON ( AssigndTo.tgtid = T.id ) 
       left join (SELECT listagg(origin_id,',') within group (order by origin_id) AS ParentDependency, 
                         target_id AS assignmentID 
                  FROM   liaison 
                  WHERE  liaison_type_name = 'assignmentDependency' 
                         AND is_deleted = 0 group by target_id) Pdep 
              ON( Pdep.assignmentid = T.id ) 
       left join (WITH childdeps (tid, childfid, parent_folder_id, title, levels1 ) 
                       AS (SELECT t.id as Tid ,I.folder_id childfid, 
                                  parent_folder_id, 
                                  f.title, 
                                  0           levels1 
                           FROM   assignmentmgr_assignment T 
                                  inner join article I 
                                          ON ( I.id = T.id ) 
                                  inner join folder F 
                                          ON ( I.folder_id = F.id ) 
                           WHERE  F.is_deleted = 0 
                           UNION ALL 
                           SELECT  Tid,childfid, 
                                  fp.parent_folder_id, 
                                  fp.title, 
                                  levels1 + 1 AS levels1 
                           FROM   folder Fp 
                                  inner join childdeps c 
                                          ON c.parent_folder_id = fp.id 
                           WHERE  fp.id LIKE 'assignmentgrp%%' and Fp.is_deleted = 0) 
                  SELECT tid, 
                         Listagg(title, ' > ') 
                           within GROUP (ORDER BY levels1 DESC ) AS path 
                   FROM   childdeps 
                   GROUP  BY Tid) F1 
              ON ( T.id = F1.Tid ) 
              where  i.is_deleted = 0
              and    F.mission_id='%s' 
              and cast(substr(T.id,5) as int) between (%s) and (%s) 
              order by T.ESTIMATION_START"""
                    printMessage("Exporting assignment data for %s"%((missionid)))
                    while int(MinKey)    <= int(MaxKey) :
                          UpperRange      =int(MinKey) + int(MaxRows)					  
                          ExportmissionSQL=  COPYSQL%(missionid,MinKey,UpperRange)
                          filename="%s_assignmentExportPart%s.csv"%(missionid,MinKey)
                          filename=os.path.join(Path,filename)
                          MinKey          =int(UpperRange)+1
                          with  open(filename,'w') as filenameop:
                                DataSet=app.query(ExportmissionSQL)
                                writer =csv.writer(filenameop)
                                writer.writerow(list(CSVHeader))
                                for row in DataSet:
                                    cols = list(row)
                                    writer.writerow(cols)
                                    logger.info( "Exporting of %s assignments completed in mission %s" %(assignmentRowCnt,missionid))
                                    logger.info( " completed Query \n %s" %(ExportmissionSQL))

            app.close()
            printMessage("Exporting assignment data completed")
            logger.info( "Exporting assignment data completed")

   except :
       print "Please refer runtime.log for any failures."
       logger.error( "Exporting assignment data failed " )



def printMessage(message):
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    print "[" + st + "]: " + message
    sys.stdout.flush()

def _handleParams():
    global Path
    global MaxRows
    Path=None
    MaxRows=None
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hp:r:d", ["help=","path=","rows=","default="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err) # will print something like "option -a not recognized"
        usage(' ')
        _exit(2)
    output  = None
    verbose = False
    option  = None

    for o, v in opts:
        if o in ( "-d","--default"):
           Path='/tmp/'
           MaxRows=4500
           option=o
        elif o in ("-h", "--help"):
            usage(' ' )
        elif o in ("-r", "--rows"): 
            if v.isdigit()  :
               MaxRows = v
               option=o
            else :
               print "invalid number for -r argument"
               _exit(0)

        elif o in ("-p", "--path"):
            if os.path.isdir(v):
               Path = v
               option=o
            else :
               print "invalid path"
               _exit(0)
        else:
            print 'invalid option refer help doc'
            usage(' ' )
    
    if (Path is None and  MaxRows is None) and (option not in ('-d','--default')):
         usage(' ' )   
    elif Path is None :
         Path='/tmp/'
    elif MaxRows is None :
         MaxRows=4500

def _exit(code):
    """ Performs proper clean up before exiting the program
    """
    sys.exit(code)


def usage(msg):
    """ Prints the tools usage to stdout
    """
    print "\nNAME"
    print "assignment export utility"
    print "\nArguments"
    print "./assignment-data-export.py [--help|-h]   ---->Information on assignment export utility "
    print "./assignment-data-export.py [--path|-p]   ---->Exported files are stored in mentioned path"
    print "./assignment-data-export.py [--rows|-r]   ---->Maximum number of rows in a file "
    print "\n"
    print "HINT:Acceptable formats to run "
    print """1)./assignment-data-export.py -r 400 -p /tmp/assignmentExport/ (Maximum 400 rows in a file at given location /tmp/assignmentExport/)\n2)./assignment-data-export.py -p /tmp/assignmentExport/ -> (Maximum 4500 rows in a file)  \n3)./assignment-data-export.py -h (Help information on utility) """
    print "\n"
    print "\nDescription"
    print "Generates csv file for each mission and csv shall be suitable to do mass import in Tracker."
    print "If a mission has more than 4500(default value is 4500 and configurable with -s)assignments then files will be generated in multiples of 4500."
    print "The location of CSV will be in tmp as a default location and configurable with -p or --path"
    print "Refer help document for importing exported assignment CSV file to do  mass import .  "
    _exit(0)
if __name__== "__main__" :
  main() 
