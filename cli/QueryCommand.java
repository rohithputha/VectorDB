package cli;

import global.AttrType;
import heap.Tuple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import diskmgr.PCounter;

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

        long startTime = System.nanoTime();

        try{
            printer("processing Query");
            String query = this.readFile(this.qsName);
            if (query.startsWith("Filter(")){
                queryCommand = new FilterQueryCommand(this.relName1, this.relName2, query);
            }
            else if (query.startsWith("Sort(")){
                queryCommand = new SortQueryCommand(this.relName1, this.relName2, query);
            }
            else if (query.startsWith("Range")){
                queryCommand = new RangeScanQueryCommand(this.relName1, this.relName2, query, false);
            }
            else if (query.startsWith("NN(")){
                queryCommand = new NNScanQueryCommand(this.relName1, this.relName2, query, false);
            }
            else if (query.startsWith("DJOIN")){
                queryCommand = new DJoinQueryCommand(this.relName1, this.relName2, query);
            }

            queryCommand.process();
        }
        catch (IOException e){
            e.printStackTrace();
            printer("Error reading query file: " + e.getMessage());
        }

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        System.out.println("Execution Time: " + (duration / 1_000_000) + " milliseconds");

        System.out.println("Read Counter Value: " + PCounter.getReads());
        System.out.println("Write Counter Value: " + PCounter.getWrites());

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
