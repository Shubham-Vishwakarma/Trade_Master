package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.AverageTradedPriceOverPastWeek;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.google.gson.JsonSyntaxException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AverageTradedPriceOverPastWeekTest extends AbstractSparkUnitTest {

    private String filePath = null;
    private Dataset<Row> trades = null;
    private AverageTradedPriceOverPastWeek averageTradedPrice = null;

    @Before
    public void setup() {

        filePath = getClass().getResource("loader-test-trades.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
        averageTradedPrice = new AverageTradedPriceOverPastWeek();
    }

    @Test
    public void AverageTradedPriceWhenTradesPresent() {

        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000A0VRQ6', " +
                "'customerId': 374587234589374958, " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        Map<RfqMetadataFieldNames, Object> result = averageTradedPrice.extractMetaData(rfq, session, trades);

        assertEquals(139.857, result.get(RfqMetadataFieldNames.averageTradedPriceOverPastWeek));
    }

    @Test
    public void AverageTradedPriceWhenInstrumentAbsent() {
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000386115', " +
                "'customerId': 374587234589374958, " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        Map<RfqMetadataFieldNames, Object> result = averageTradedPrice.extractMetaData(rfq, session, trades);

        assertEquals(0.0, result.get(RfqMetadataFieldNames.averageTradedPriceOverPastWeek));
    }

    @Test
    public void AverageTradedPriceWhenEntityAbsent() {
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690943, " +
                "'instrumentId': 'AT0000386115', " +
                "'customerId': 374587234589374958, " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        Map<RfqMetadataFieldNames, Object> result = averageTradedPrice.extractMetaData(rfq, session, trades);

        assertEquals(0.0, result.get(RfqMetadataFieldNames.averageTradedPriceOverPastWeek));
    }

    @Test(expected = JsonSyntaxException.class)
    public void AverageTradedPriceWhenRfqIncomplete() {
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': , " +
                "'instrumentId': , " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'customerId': 374587234589374958, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        Map<RfqMetadataFieldNames, Object> result = averageTradedPrice.extractMetaData(rfq, session, trades);
    }

    @After
    public void teardown() {
        trades = null;
        averageTradedPrice = null;
    }
}
