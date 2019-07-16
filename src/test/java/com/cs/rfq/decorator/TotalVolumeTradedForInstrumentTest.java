package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.TotalVolumeTradedForInstrumentExtractor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TotalVolumeTradedForInstrumentTest extends AbstractSparkUnitTest {
    @Test
    public void VolumeMetadataTest(){
        String filePath = getClass().getResource("loader-test-trades.json").getPath();

        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000A0VRQ6', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        //Total is 850000
        TotalVolumeTradedForInstrumentExtractor totalVolumeTradedForInstrumentExtractor = new TotalVolumeTradedForInstrumentExtractor();
        Map<RfqMetadataFieldNames,Object> volMap = totalVolumeTradedForInstrumentExtractor.extractMetaData(rfq, session, trades);
        Assert.assertEquals(850000,volMap.get(RfqMetadataFieldNames.volumeTradedForInstrument));
    }
}
