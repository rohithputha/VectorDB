package cli;

import java.util.Scanner;

public class Shell implements VectorDbCommand {
    private String command;

    private Shell(String command){
        this.command = command;
    }
    private Shell(){
        this.command = "";
    }
    private void setCommand(String command){
        this.command = command;
    }

    public static void main(String[] args) {
        Shell shell = new Shell("");
        Scanner scanner = new Scanner(System.in);
        System.out.println("Welcome to VectorDB CLI. Type 'exit' to quit.");
        shell.process();
    }
    private void handleCommand(String input) {
        BaseCommand baseCommand;
        try {
            String[] commands = input.trim().split(" ");
            for (int i = 0; i < commands.length; i++) {
                commands[i] = commands[i].trim();
            }
            baseCommand = new BaseCommand(commands);
        }
        catch (Exception e) {
            return;
        }
        baseCommand.process();
    }

    @Override
    public String getCommand() {
        return this.command;
    }

    @Override
    public void process() {
        while (true) {
            this.printer();
            String input = this.receive();
            this.setCommand(input);
            if (input.isEmpty()){
                continue;
            }
            if (input.equalsIgnoreCase("exit")) {
                printer("Exiting VectorDB CLI...");
                break;
            }

            handleCommand(input);
        }
        this.close();
    }
}
