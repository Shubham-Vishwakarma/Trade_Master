package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class TotalVolumeTradedForInstrumentExtractor implements RfqMetadataExtractor{

    private String since;

    public TotalVolumeTradedForInstrumentExtractor() {
        this.since = DateTime.now().getYear() + "-01-01";
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        return null;
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades, Dataset<Row> rfqs) {
        String query = String.format("SELECT sum(trade.LastQty) from trade JOIN rfqt ON trade.OrderID = rfqt.id where trade.SecurityID='%s' AND rfqt.customerId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                rfq.getCustomerId(),
                since);

        trades.createOrReplaceTempView("trade");
        rfqs.createOrReplaceTempView("rfqt");

        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.volumeTradedForInstrument, volume);
        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }
}
