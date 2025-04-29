package cli;

import global.SystemDefs;
import diskmgr.PCounter;

public class CloseDatabaseCommand implements VectorDbCommand{
    private String command;
    public static final String COMMAND = "close_database";
    public CloseDatabaseCommand(String[] args) {
        this.command = args[0];
    }
    @Override
    public String getCommand() {
        return this.command;
    }

    @Override
    public void process() {

        long startTime = System.nanoTime();

        try{
            if (this.getEnvironment().getDb()!= null){
                SystemDefs.JavabaseDB.closeDB();
                this.getEnvironment().setDb(null);
            }
            else {
                throw new Exception("Database is not open.");
            }
        }
        catch(Exception e){
            printer("Error closing DB: "+e.getMessage());
            return;
        }

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        System.out.println("Execution Time: " + (duration / 1_000_000) + " milliseconds");

        System.out.println("Read Counter Value: " + PCounter.getReads());
        System.out.println("Write Counter Value: " + PCounter.getWrites());
    }
}
