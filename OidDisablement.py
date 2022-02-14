import commands
import os
oiddir='/tmp/'
def disableOID():
        cmd = """psql -U postgres   -A  -t -c "SELECT datname FROM pg_database where datname not like 'template%' and datname not in ('postgres')" """
        (exitCode, dbs) = commands.getstatusoutput(cmd)
        if len(dbs) > 0 and exitCode == 0:
            listdbs = dbs.split()
            for db in listdbs:
                cmd = """psql -U postgres %s -A -t -c "SELECT ' ALTER TABLE ' ||table_name||' set without OIDS;' FROM information_schema.tables where table_schema='public' " """ % db
                (exitCode, sqls) = commands.getstatusoutput(cmd)
                ddl = " echo '%s' | psql -U postgres %s " % (sqls, db)
                (exitCode, dbs) = commands.getstatusoutput(ddl)

        exitCode = os.system("mkdir -p %s" % oiddir)
        open(oiddir + '/oid.done', 'w').close()
        print('***process completed successfully***')