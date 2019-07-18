package com.cs.rfq.utils;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.RfqDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RfqHistorySimulator {

    private static final String rfqs_file = "src/test/resources/rfqs/rfqs.json";
    private Rfq rfq;
    private Dataset<Row> rfqDataset;

    public RfqHistorySimulator(Rfq rfq){
        this.rfq = rfq;
    }
    public RfqHistorySimulator(){

    }

    public void logRfq(){
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(Paths.get(rfqs_file).toFile(),true));
            out.println(rfq.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        out.flush();
        out.close();
    }

    public String fetchRfq(SparkSession session){
        this.rfqDataset = new RfqDataLoader().loadTrades(session, "src/test/resources/rfqs/*.json");
        File rfqFile = new File("src\\test\\resources\\rfqs\\rfqs.json");
        //rfqs/rfqs.json
        long len = rfqFile.length();
        long rand = (long) (Math.random()*len );
        String lastRfq = "";
        RandomAccessFile rFile = null;
        try {
            rFile = new RandomAccessFile(rfqFile, "rws");
            rFile.seek(rand);
            lastRfq = rFile.readLine();
            lastRfq = rFile.readLine();
        }catch(Exception ex){
            ex.printStackTrace();
        }
        System.out.println("ATTN : " + lastRfq);
        return lastRfq;
    }

    public List<String> fetchRfqsFromFile(SparkSession session, String filename){
        List<String> myrfqs = null;
        try {
            myrfqs = Files.readAllLines(Paths.get("src\\test\\resources\\rfqs\\"+ filename +".json"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return myrfqs;
    }
}
