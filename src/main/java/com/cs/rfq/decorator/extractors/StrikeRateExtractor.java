package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class StrikeRateExtractor implements RfqMetadataExtractor {
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        return null;
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades, Dataset<Row> rfqs) {
//        String query1 = String.format("SELECT count(*) from rfqt where customerId='%s'",
//                rfq.getCustomerId());
//        String query2 = String.format("SELECT count(*) from trade where OrderID in (SELECT id from rfqt where customerId='%s')",
//                rfq.getCustomerId());

        String query3 = String.format("SELECT (SELECT count(*) from trade where OrderID in (SELECT id from rfqt where customerId='%s'))/" +
                "(SELECT count(*) from rfqt where customerId='%s')",rfq.getCustomerId(),rfq.getCustomerId());

        trades.createOrReplaceTempView("trade");
        rfqs.createOrReplaceTempView("rfqt");

//        Dataset<Row> sqlQuery1Result = session.sql(query1);
//        Dataset<Row> sqlQuery2Result = session.sql(query2);
        Dataset<Row> sqlQuery3Result = session.sql(query3);

//        System.out.println("ATTN : " + sqlQuery1Result.first().get(0));
//        System.out.println(sqlQuery2Result.first().get(0));
//        System.out.println(sqlQuery3Result.first().get(0));

//        Object tradeCount = sqlQuery1Result.first().get(0);
//        if (tradeCount == null) {
//            tradeCount = 0L;
//        }

//        Object rfqCount = sqlQuery2Result.first().get(0);
//        if (rfqCount == null) {
//            rfqCount = 0L;
//        }

        Object expectedCount = sqlQuery3Result.first().get(0);
        if (expectedCount == null) {
            expectedCount = 0L;
        }

//        double str = Double.valueOf(tradeCount.toString())/Double.valueOf(rfqCount.toString())*100;
        double str = Double.valueOf(expectedCount.toString())*100;

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.strikeRate, str);
        return results;

    }
}
