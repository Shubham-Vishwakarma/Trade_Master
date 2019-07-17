package dev.testdata;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;
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

    long customers[] = {7613335221288713701L,7673223210684687219L,1436619279973149617L,914697713863714686L,7972594512212812145L,6147326492973788804L,1060337766161584173L,5734591245048622482L,4823387127763137439L,2526969513165927461L};

    long Id ;
    long TraderId;
    long EntityId;
    String instrumentID = "";// isin value here
    long qty = 0;//1000000 = 1m
    double price = 0;//float value - may be negative
    String Side = "B"; //: 1 = buy, 2 = sell
    long customerId = customers[(int) (Math.random()*10)];


    public Rfq(Counterparty c, Instrument i) {
        Id =  Math.abs(r.nextLong()) ;
        TraderId = c.traderId;
        EntityId = c.entityId;
        instrumentID = i.isin;
        qty = 50_000 * (int) ((Math.random() * 10) + 1); // 1-10 * 50_000
        price = randomizePrice(i.price);
        Side = (Math.random() > 0.5) ? "B" : "S";
    }

    public double getRfqPrice(){
        return price;
    }

    public long getQuantity(){
        return qty;
    }

    public int getRfqSide(){
        if(Side.equals("B")){return 1;}
        else{return 2;}
    }

    public long getRfqId(){
        return Id;
    }

    private static double randomizePrice(double start) {
        // will be +/- 0.5 rounded to 3dp
        start += (0.5 * Math.random()) * (Math.random() > 0.5 ? 1 : -1);
        start *= 1000;
        int result = (int) start;
        return result / 1000.0;
    }

    //counter party , instrument , orderId, price, quantity,side

    @Override
    public String toString() {
        return "{" +
                "'id':"+ Id+
                ", 'traderId':" + TraderId +
                ", 'entityId':" + EntityId +
                ", 'instrumentId':" + "'"+instrumentID+"'" +
                ", 'qty':" + qty +
                ", 'price':" + price +
                ", 'Side':" + "'"+Side+"'" +
                ".'customerId':"+ customerId+
                '}';
    }
}
