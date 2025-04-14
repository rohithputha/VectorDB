package cli;

import diskmgr.DB;
import global.SystemDefs;

public class Environment {
    private String dbName;
    private SystemDefs systemDefs;
    public void setDb(String dbName){
        this.dbName = dbName;
        // set the db as well
    }
    public String getDb(){
        return this.dbName;
    }

    public void setSystemDefs(SystemDefs systemDefs){
        this.systemDefs = systemDefs;
    }
}
