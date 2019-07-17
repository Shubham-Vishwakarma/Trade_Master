package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.TotalVolumeTradedForInstrumentExtractor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class TotalVolumeTradedForInstrumentExtractorTest extends AbstractSparkUnitTest {

    String filePath;
    String filePathRfqs;
    Dataset<Row> trades;
    Dataset<Row> rfqs;
    TotalVolumeTradedForInstrumentExtractor totalVolumeTradedForInstrumentExtractor;

    @Before
    public void setup(){
        filePath = getClass().getResource("loader-test-trades.json").getPath();
        filePathRfqs = getClass().getResource("loader-test-rfq.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
        rfqs = new RfqDataLoader().loadTrades(session, filePathRfqs);
        totalVolumeTradedForInstrumentExtractor = new TotalVolumeTradedForInstrumentExtractor();
    }

    @Test
    public void VolumeMetadataTestValidInstruments(){

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
        Map<RfqMetadataFieldNames,Object> volMap = totalVolumeTradedForInstrumentExtractor.extractMetaData(rfq, session, trades, rfqs);
        Assert.assertEquals(Long.valueOf(850000),volMap.get(RfqMetadataFieldNames.volumeTradedForInstrument));
    }

    @Test
    public void VolumeMetadataTestInvalidInstruments(){
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
        Map<RfqMetadataFieldNames,Object> volMap = totalVolumeTradedForInstrumentExtractor.extractMetaData(rfq, session, trades,rfqs);
        Assert.assertEquals(Long.valueOf(0),volMap.get(RfqMetadataFieldNames.volumeTradedForInstrument));
    }

    @Test
    public void VolumeMetadataTestInvalidCustomer(){
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'customerId': 2, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'BT0000A0VRQ6', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        //Total is 0
        Map<RfqMetadataFieldNames,Object> volMap = totalVolumeTradedForInstrumentExtractor.extractMetaData(rfq, session, trades,rfqs);
        Assert.assertEquals(Long.valueOf(0),volMap.get(RfqMetadataFieldNames.volumeTradedForInstrument));
    }
}
