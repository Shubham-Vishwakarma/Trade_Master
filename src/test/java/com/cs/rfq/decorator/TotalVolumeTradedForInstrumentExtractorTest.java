package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.TotalVolumeTradedForInstrumentExtractor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TotalVolumeTradedForInstrumentExtractorTest extends AbstractSparkUnitTest {
    @Test
    public void VolumeMetadataTestValidInstruments(){
        String filePath = getClass().getResource("loader-test-trades.json").getPath();
        String filePathRfqs = getClass().getResource("loader-test-rfq.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);
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

        //Total is 850000
        TotalVolumeTradedForInstrumentExtractor totalVolumeTradedForInstrumentExtractor = new TotalVolumeTradedForInstrumentExtractor();
        Map<RfqMetadataFieldNames,Object> volMap = totalVolumeTradedForInstrumentExtractor.extractMetaData(rfq, session, trades, rfqs);
        Assert.assertEquals(Long.valueOf(850000),volMap.get(RfqMetadataFieldNames.volumeTradedForInstrument));
    }

    @Test
    public void VolumeMetadataTestInvalidInstruments(){
        String filePath = getClass().getResource("loader-test-trades.json").getPath();

        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'customerId': 101120022012, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'BT0000A0VRQ6', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        //Total is 0
        TotalVolumeTradedForInstrumentExtractor totalVolumeTradedForInstrumentExtractor = new TotalVolumeTradedForInstrumentExtractor();
        Map<RfqMetadataFieldNames,Object> volMap = totalVolumeTradedForInstrumentExtractor.extractMetaData(rfq, session, trades);
        Assert.assertEquals(Long.valueOf(0),volMap.get(RfqMetadataFieldNames.volumeTradedForInstrument));
    }
}
