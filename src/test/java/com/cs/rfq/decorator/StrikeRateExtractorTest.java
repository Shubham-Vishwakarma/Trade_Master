package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.StrikeRateExtractor;
import org.apache.commons.math3.util.Precision;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class StrikeRateExtractorTest extends AbstractSparkUnitTest {

    String filePathTrades;
    String filePathRfqs;
    Dataset<Row> trades;
    Dataset<Row> rfqs;
    StrikeRateExtractor strikeRateExtractor;

    @Before
    public void setup(){
        filePathTrades = getClass().getResource("loader-test-trades.json").getPath();
        filePathRfqs = getClass().getResource("loader-test-rfq.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePathTrades);
        rfqs = new RfqDataLoader().loadTrades(session, filePathRfqs);
        strikeRateExtractor = new StrikeRateExtractor();
    }

    @Test
    public void StrikerRateTestFirstCustomer(){

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

        Map<RfqMetadataFieldNames,Object> srMap = strikeRateExtractor.extractMetaData(rfq, session, trades, rfqs);
        Assert.assertEquals(Precision.round(Double.valueOf(400.0/6),2),Precision.round(Double.valueOf(srMap.get(RfqMetadataFieldNames.strikeRate).toString()),2),0);
    }

    @Test
    public void StrikeRateTestSecondCustomer(){
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

        Map<RfqMetadataFieldNames,Object> srMap = strikeRateExtractor.extractMetaData(rfq, session, trades, rfqs);
        Assert.assertEquals(Double.valueOf(Precision.round(200.0/6,2)),Double.valueOf(Precision.round(Double.valueOf(srMap.get(RfqMetadataFieldNames.strikeRate).toString()),2)));
    }

    @Test
    public void StrikeRateTestInvalidCustomer(){
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

        Map<RfqMetadataFieldNames,Object> srMap = strikeRateExtractor.extractMetaData(rfq, session, trades, rfqs);
        Assert.assertEquals(Double.valueOf(0),srMap.get(RfqMetadataFieldNames.strikeRate));
    }
}
