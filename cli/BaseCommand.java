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
            case BatchCreateCommand.COMMAND:
                this.commandObj = new BatchCreateCommand(args);
                break;
            case CreateIndex.COMMAND:
                this.commandObj = new CreateIndex(args);
                break;
            case BatchInsertCommand.COMMAND:
                this.commandObj = new BatchInsertCommand(args);
                break;
            case BatchDeleteCommand.COMMAND:
                this.commandObj = new BatchDeleteCommand(args);
                break;
            case QueryCommand.COMMAND:
                this.commandObj = new QueryCommand(args);
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
