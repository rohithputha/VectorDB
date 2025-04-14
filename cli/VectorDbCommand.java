package cli;

import java.util.Scanner;

public interface VectorDbCommand {
    String getCommand();
    void process();
    Environment env = new Environment();
    Scanner scanner = new Scanner(System.in);

    default void printer(String message){
        System.out.print("VectorDB> ");
        System.out.println(message);
    }
    default void printer(){
        System.out.print("VectorDB> ");
    }
    default Environment getEnvironment(){
        return env;
    }
    default String receive(){
        return scanner.nextLine().trim();
    }
    default void close(){
        scanner.close();
    }
}
