package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.TradeSideBias;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TradeSideBiasTest extends AbstractSparkUnitTest {

    String path = null;
    Dataset<Row> trades = null;
    TradeSideBias tradeSideBias = null;

    @Before
    public void setup() {
        path = getClass().getResource("loader-test-trades.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, path);
        tradeSideBias = new TradeSideBias();
    }

    @Test
    public void TradeSideBiasInstrumentTraded() {
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000A0VRQ6', " +
                "'qty': 250000, " +
                "'customerId': 374587234589374958, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        Map<RfqMetadataFieldNames, Object> result = tradeSideBias.extractMetaData(rfq, session, trades);

        assertEquals("2:0", result.get(RfqMetadataFieldNames.buyUponSellForWeek));
        assertEquals("2:0", result.get(RfqMetadataFieldNames.buyUponSellForMonth));
    }

    @Test
    public void TradeSideBiasInstrumentNotTraded() {
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000A0VRAW', " +
                "'qty': 250000, " +
                "'customerId': 374587234589374958, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        Map<RfqMetadataFieldNames, Object> result = tradeSideBias.extractMetaData(rfq, session, trades);

        assertEquals("-1", result.get(RfqMetadataFieldNames.buyUponSellForWeek));
        assertEquals("-1", result.get(RfqMetadataFieldNames.buyUponSellForMonth));
    }
}
