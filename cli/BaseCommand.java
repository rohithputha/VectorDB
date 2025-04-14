package cli;

public class BaseCommand implements VectorDbCommand {

    private String command;

    private VectorDbCommand commandObj;

    public BaseCommand(String[] args) {
        command = args[0];
        switch (command) {
            case OpenDatabaseCommand.COMMAND:
                this.commandObj = new OpenDatabaseCommand(args);
                break;
            case CloseDatabaseCommand.COMMAND:
                this.commandObj = new CloseDatabaseCommand(args);
                break;
            default:
                printer("Unknown command: " + command);
                throw new IllegalArgumentException("Unknown command: " + command);
        }
    }

    @Override
    public String getCommand() {
        return this.command;
    }

    @Override
    public void process() {
        this.commandObj.process();
    }

}
