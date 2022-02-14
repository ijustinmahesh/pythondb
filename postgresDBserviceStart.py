
import commands
import os
import sys

manual_start = False
cmd_bin_datadir = ""
pg_commands = ["start\" ", "stop\" ", "status\" "]
pgstatus = ""




def __manual_start_pg():
    global manual_start
    global cmd_bin_datadir
    global pgstatus
    msg = "command to start the postgres :/usr/pgsql-11/bin/pg_ctl -D /opt/netverse/teamworld/var/pgsql/11/data start"
    cmd = """ su - postgres -c "/usr/pgsql-11/bin/pg_ctl -D /opt/netverse/teamworld/var/pgsql/11/data status" """
    (exitCode, dbstatus) = commands.getstatusoutput(cmd)
    root_dir = os.path.join(app.defaults.DEFAULT_DATA_DIR, "pgsql")
    if os.path.exists("/usr/pgsql-11/bin/pg_ctl"):
        bin_dir = """ su - postgres -c "/usr/pgsql-11/bin/pg_ctl -w -D"""
        if os.path.exists(os.path.join(root_dir, "11", "data")):
            cmd_bin_datadir = bin_dir + """ /opt/netverse/teamworld/var/pgsql/11/data  """
            (exitCode, dbstatus) = commands.getstatusoutput(cmd_bin_datadir + pg_commands[2])
            if dbstatus.find("PID") == -1:
                dbstatus = app.util.executeCommand(cmd_bin_datadir + pg_commands[0] + "&> /dev/null")
                manual_start = True
        elif os.path.exists(os.path.join(root_dir, "11.6", "data")):
            cmd_bin_datadir = bin_dir + """ /opt/netverse/teamworld/var/pgsql/11.6/data  """
            (exitCode, dbstatus) = commands.getstatusoutput(cmd_bin_datadir + pg_commands[2])
            if dbstatus.find("PID") == -1:
                dbstatus = app.util.executeCommand(cmd_bin_datadir + pg_commands[0] + "&> /dev/null")
                manual_start = True
        elif os.path.exists(os.path.join(root_dir, "11.12", "data")):
            cmd_bin_datadir = bin_dir + """ /opt/netverse/teamworld/var/pgsql/11.12/data  """
            (exitCode, dbstatus) = commands.getstatusoutput(cmd_bin_datadir + pg_commands[2])
            if dbstatus.find("PID") == -1:
                dbstatus = app.util.executeCommand(cmd_bin_datadir + pg_commands[0] + "&> /dev/null")
                manual_start = True
        elif os.path.exists(os.path.join(root_dir, "11.1", "data")):
            cmd_bin_datadir = bin_dir + """ /opt/netverse/teamworld/var/pgsql/11.1/data  """
            (exitCode, dbstatus) = commands.getstatusoutput(cmd_bin_datadir + pg_commands[2])
            if dbstatus.find("PID") == -1:
                dbstatus = app.util.executeCommand(cmd_bin_datadir + pg_commands[0] + "&> /dev/null")
                manual_start = True
        else:
            print(
                "Unable to locate  data directory hence  please find  correct data directory location and use similar command given below ")
            print(msg)
            sys.exit(0)
        pgstatus = "OK"
    else:
        print("Unable to locate pgsql 11 bin dir.Find bin and data dir and use below command ")
        print(msg)
        sys.exit(0)




if __name__ == '__main__':
    dbs()