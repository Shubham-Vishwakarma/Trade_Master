package com.cs.rfq.utils;

import com.cs.rfq.decorator.Rfq;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;

public class RfqHistorySimulator {

    private static final String rfqs_file = "src/test/resources/rfqs/rfqs.json";
    private Rfq rfq;

    public RfqHistorySimulator(Rfq rfq){
        this.rfq = rfq;
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
}
