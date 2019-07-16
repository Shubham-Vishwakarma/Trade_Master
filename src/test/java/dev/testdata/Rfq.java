package dev.testdata;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Random;

public class Rfq {
    private static Random r = new Random();
    //        {
//                'id': 9315444593154445,
//                'traderId': 3351266293154445953,
//                'entityId': 5561279226039690843,
//                'instrumentId': 'AT0000383864',
//                'qty': 250000,
//                'price': 1.58,
//                'side': 'B'
//        }
    //counterparty IDs
    long id =Math.abs(r.nextLong());
    long TraderId;
    long EntityId;
    String instrumentID = "";// isin value here
    long qty = 0;//1000000 = 1m
    double price = 0;//float value - may be negative
    char Side = 'B'; //: 1 = buy, 2 = sell

    DateTimeFormatter tradeDateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
    DateTimeFormatter transactTimeFormatter = DateTimeFormat.forPattern("YYYYMMdd-HH:mm:ss");

    public Rfq(Counterparty c, Instrument i) {
        TraderId = c.traderId;
        EntityId = c.entityId;
        instrumentID = i.isin;
        qty = 50_000 * (int) ((Math.random() * 10) + 1); // 1-10 * 50_000
        price = randomizePrice(i.price);
        Side = (Math.random() > 0.5) ? 'B' : 'S';
    }

    private static double randomizePrice(double start) {
        // will be +/- 0.5 rounded to 3dp
        start += (0.5 * Math.random()) * (Math.random() > 0.5 ? 1 : -1);
        start *= 1000;
        int result = (int) start;
        return result / 1000.0;
    }

    @Override
    public String toString() {
        return "{" +
                "'id':"+ id+
                ", 'traderId':" + TraderId +
                ", 'entityId':" + EntityId +
                ", 'instrumentId':" + instrumentID +
                ", 'qty':" + qty +
                ", 'price':" + price +
                ", 'Side':" + Side +
                '}';
    }
}
