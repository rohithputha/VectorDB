package cli;

import global.SystemDefs;

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
    }
}
