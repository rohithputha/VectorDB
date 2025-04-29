package cli;

import global.GlobalConst;
import global.SystemDefs;
import diskmgr.PCounter;

import java.io.File;

public class OpenDatabaseCommand implements VectorDbCommand{

    private String commandName;
    private String dbName;
    public static final String COMMAND = "open_database";
    public OpenDatabaseCommand(String[] args) {
        this.commandName = args[0];
        this.dbName = args[1];
    }
    @Override
    public String getCommand() {
        return this.commandName;
    }

    @Override
    public void process() {

        long startTime = System.nanoTime();

        SystemDefs systemDefs = null;
        if (this.getEnvironment().getDb() != null){
            new BaseCommand(new String[]{CloseDatabaseCommand.COMMAND}).process();
        }
        if (ifDbFileExists(this.dbName)) {
            printer("Database already exists. opening it.");
            systemDefs = new SystemDefs(this.dbName, 0, GlobalConst.MINIBASE_BUFFER_POOL_SIZE, "Clock");
        }
        else{
            printer("Database does not exist. creating a new one.");
            systemDefs = new SystemDefs(this.dbName, GlobalConst.MINIBASE_DB_SIZE, GlobalConst.MINIBASE_BUFFER_POOL_SIZE, "Clock");
        }
        this.getEnvironment().setDb(this.dbName);
        this.getEnvironment().setSystemDefs(systemDefs);

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        System.out.println("Execution Time: " + (duration / 1_000_000) + " milliseconds");

        System.out.println("Read Counter Value: " + PCounter.getReads());
        System.out.println("Write Counter Value: " + PCounter.getWrites());
    }

    private boolean ifDbFileExists(String dbFile) {
        File f = new File(dbFile);
        return f.exists();
    }
}
