package cli;

import global.AttrType;
import heap.Tuple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class QueryCommand implements VectorDbCommand {

    private final String command;
    private final String relName1;
    private final String relName2;
    private final String qsName;
    private final int numBuffs;

    private VectorDbCommand queryCommand;

    public static final String COMMAND = "query";

    protected List<Tuple> results;
    protected AttrType[] projectAttrSchema;
    public QueryCommand(String[] args) {
        this.command = args[0];
        this.relName1 = args[1];
        this.relName2 = args[2];
        this.qsName = args[3];
        this.numBuffs = Integer.parseInt(args[4]);
    }
    @Override
    public String getCommand() {
        return command;
    }

    @Override
    public void process() {
        try{
            printer("processing Query");
            String query = this.readFile(this.qsName);
            if (query.startsWith("Filter(")){
                queryCommand = new FilterQueryCommand(this.relName1, this.relName2, query);
            }
            else if (query.startsWith("Sort(")){
                // set the query command to the sort class
            }

            queryCommand.process();
        }
        catch (IOException e){
            e.printStackTrace();
            printer("Error reading query file: " + e.getMessage());
        }

    }

    private String readFile(String path) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line)
                        .append(System.lineSeparator()); // preserve line breaks
            }
        }
        return sb.toString();
    }
}
