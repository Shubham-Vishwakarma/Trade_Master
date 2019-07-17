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
        String query1 = String.format("SELECT count(*) from trade where EntityId='%s'",
                rfq.getEntityId());
        String query2 = String.format("SELECT count(*) from rfqt where entityId='%s'",
                rfq.getEntityId());

        trades.createOrReplaceTempView("trade");
        rfqs.createOrReplaceTempView("rfqt");

        Dataset<Row> sqlQuery1Result = session.sql(query1);
        Dataset<Row> sqlQuery2Result = session.sql(query2);

        Object tradeCount = sqlQuery1Result.first().get(0);
        if (tradeCount == null) {
            tradeCount = 0L;
        }

        Object rfqCount = sqlQuery2Result.first().get(0);
        if (rfqCount == null) {
            rfqCount = 0L;
        }

        double str = (double)tradeCount/(double)rfqCount*100;

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.strikeRate, str);
        return results;

    }
}
