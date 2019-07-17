package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import java.util.HashMap;
import java.util.Map;

public class InstrumentLiquidityExtractor implements RfqMetadataExtractor {
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        //long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        //String pastMonthMs = DateTime.now().withMillis(todayMs).minusMonths(1);
        LocalDate pastMonthMs = LocalDate.now().minusMonths(1);

        System.out.println("ATTN :" + pastMonthMs);

        String query = String.format("SELECT sum(LastQty) from trade  where SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                pastMonthMs);

        trades.createOrReplaceTempView("trade");

        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.volumeTradedForInstrumentPastMonth, volume);
        return results;
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades, Dataset<Row> rfqs) {
        return null;
    }
}
