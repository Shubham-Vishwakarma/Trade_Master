package com.cs.rfq.decorator;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.AverageTradedPriceOverPastWeek;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AverageTradedPriceOverPastWeekTest extends AbstractSparkUnitTest {

    @Test
    public void AverageTradedPriceWhenTradesPresent() {
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

        AverageTradedPriceOverPastWeek AverageTradedPrice = new AverageTradedPriceOverPastWeek();

        Map<RfqMetadataFieldNames, Object> averageTradedPrice = AverageTradedPrice.extractMetaData(rfq, session, trades);

        assertEquals((Double) 139.857, averageTradedPrice.get(RfqMetadataFieldNames.averageTradedPriceOverPastWeek));
    }
}
