package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.StrikeRateExtractor;
import com.cs.rfq.decorator.extractors.TotalVolumeTradedForInstrumentExtractor;
import org.apache.commons.math3.util.Precision;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class StrikeRateExtractorTest extends AbstractSparkUnitTest {
    @Test
    public void StrikerRateTestFirstCustomer(){
        String filePathTrades = getClass().getResource("loader-test-trades.json").getPath();
        String filePathRfqs = getClass().getResource("loader-test-rfq.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePathTrades);
        Dataset<Row> rfqs = new RfqDataLoader().loadTrades(session, filePathRfqs);
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'customerId': 101120022012, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000A0VRQ6', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        StrikeRateExtractor strikeRateExtractor = new StrikeRateExtractor();
        Map<RfqMetadataFieldNames,Object> srMap = strikeRateExtractor.extractMetaData(rfq, session, trades, rfqs);
        Assert.assertEquals(Double.valueOf(60),srMap.get(RfqMetadataFieldNames.strikeRate));
    }

    @Test
    public void StrikeRateTestSecondCustomer(){
        String filePathTrades = getClass().getResource("loader-test-trades.json").getPath();
        String filePathRfqs = getClass().getResource("loader-test-rfq.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePathTrades);
        Dataset<Row> rfqs = new RfqDataLoader().loadTrades(session, filePathRfqs);
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'customerId': 234530022223, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000A0N9A0', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        StrikeRateExtractor strikeRateExtractor = new StrikeRateExtractor();
        Map<RfqMetadataFieldNames,Object> srMap = strikeRateExtractor.extractMetaData(rfq, session, trades, rfqs);
        Assert.assertEquals(Double.valueOf(Precision.round(200.0/6,2)),Double.valueOf(Precision.round(Double.valueOf(srMap.get(RfqMetadataFieldNames.strikeRate).toString()),2)));
    }

    @Test
    public void StrikeRateTestInvalidCustomer(){
        String filePath = getClass().getResource("loader-test-trades.json").getPath();

        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        String filePathRfqs = getClass().getResource("loader-test-rfq.json").getPath();
        Dataset<Row> rfqs = new RfqDataLoader().loadTrades(session, filePathRfqs);
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'customerId': 234530022200, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'BT0000A0VRQ6', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        //Total is 0
        StrikeRateExtractor strikeRateExtractor = new StrikeRateExtractor();
        Map<RfqMetadataFieldNames,Object> srMap = strikeRateExtractor.extractMetaData(rfq, session, trades, rfqs);
        Assert.assertEquals(Double.valueOf(0),srMap.get(RfqMetadataFieldNames.strikeRate));
    }
}
