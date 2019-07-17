package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.InstrumentLiquidityExtractor;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.StrikeRateExtractor;
import org.apache.commons.math3.util.Precision;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class InstrumentLiquidityExtractorTest extends AbstractSparkUnitTest {

    String filePathTrades;
    String filePathRfqs;
    Dataset<Row> trades;
    Dataset<Row> rfqs;
    InstrumentLiquidityExtractor instrumentLiquidityExtractor;

    @Before
    public void setup(){
        filePathTrades = getClass().getResource("loader-test-trades.json").getPath();
        filePathRfqs = getClass().getResource("loader-test-rfq.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePathTrades);
        rfqs = new RfqDataLoader().loadTrades(session, filePathRfqs);
        instrumentLiquidityExtractor = new InstrumentLiquidityExtractor();
    }

    @Test
    public void LiquidityTestFirstInstrument(){

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

        Map<RfqMetadataFieldNames,Object> liqMap = instrumentLiquidityExtractor.extractMetaData(rfq, session, trades);
        Assert.assertEquals(Long.valueOf(850000),liqMap.get(RfqMetadataFieldNames.volumeTradedForInstrumentPastMonth));
    }

    @Test
    public void LiquidityTestSecondInstrument(){
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

        Map<RfqMetadataFieldNames,Object> liqMap = instrumentLiquidityExtractor.extractMetaData(rfq, session, trades);
        Assert.assertEquals(Long.valueOf(50000),liqMap.get(RfqMetadataFieldNames.volumeTradedForInstrumentPastMonth));
    }

    @Test
    public void LiquidityTestInvalidInstrument(){
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'customerId': 234530022200, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AAAA', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        Map<RfqMetadataFieldNames,Object> liqMap = instrumentLiquidityExtractor.extractMetaData(rfq, session, trades);
        Assert.assertEquals(null,liqMap.get(RfqMetadataFieldNames.strikeRate));
    }
}
