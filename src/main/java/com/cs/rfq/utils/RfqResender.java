package com.cs.rfq.utils;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.RfqDataLoader;
import com.cs.rfq.decorator.RfqProcessor;
import com.cs.rfq.decorator.TradeDataLoader;
import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;

public class RfqResender {
    static List<RfqMetadataExtractor> extractors = new ArrayList<>();
    static List<RfqMetadataExtractor> extractor2 = new ArrayList<>();
    static Dataset<Row> trades;
    static Dataset<Row> rfqs;
    static MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public static void main(String[] Args){
       System.out.println("1. Pick Random rfq from file \n2.Enter File Name ");
       RfqHistorySimulator simulator = new RfqHistorySimulator();
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new AverageTradedPriceOverPastWeek());
        extractors.add(new TradeSideBias());
        extractors.add(new InstrumentLiquidityExtractor());
        extractor2.add(new VolumeTradedWithEntityYTDExtractor());
        extractor2.add(new TotalVolumeTradedForInstrumentExtractor());
        extractor2.add(new StrikeRateExtractor());
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");

        //TODO: create a Spark configuration and set a sensible app name
        SparkConf conf = new SparkConf().setAppName("RfqDecorator");
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        trades = new TradeDataLoader().loadTrades(session, "src/test/resources/trades/*.json");
        rfqs = new RfqDataLoader().loadTrades(session, "src/test/resources/rfqs/*.json");

        Scanner sc = new Scanner(System.in);
        int choice = sc.nextInt();

        switch (choice){
            case 1:
                String rfqString = simulator.fetchRfq(SparkSession.builder().getOrCreate());
                printRfqMetadataFromRfqString(rfqString, session);
//                Rfq rfq = Rfq.fromJson(rfqString);
//                //create a blank map for the metadata to be collected
//                Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();
//
//                //TODO: get metadata from each of the extractors
//                extractors.forEach(e -> {
//                    metadata.putAll(e.extractMetaData(rfq, session, trades));
//                });
//
//                extractor2.forEach(e -> {
//                    metadata.putAll(e.extractMetaData(rfq, session, trades, rfqs));
//                });
//
//                //TODO: publish the metadata
//                publisher.publishMetadata(metadata);
                break;

            case 2:
                System.out.println("Please specify the file name (without.json) : ");
                String filename = sc.nextLine();
                filename = sc.nextLine();
                List <String> rfqList = simulator.fetchRfqsFromFile(session,filename);
//                for(String rfqStringFromList : rfqList){
//                    printRfqMetadataFromRfqString(rfqStringFromList, session);
//                }
                rfqList.forEach(item->printRfqMetadataFromRfqString(item,session));
                break;
        }
    }

    public static void printRfqMetadataFromRfqString(String rfqString, SparkSession session){
        Rfq rfq = Rfq.fromJson(rfqString);
        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        //TODO: get metadata from each of the extractors
        extractors.forEach(e -> {
            metadata.putAll(e.extractMetaData(rfq, session, trades));
        });

        extractor2.forEach(e -> {
            metadata.putAll(e.extractMetaData(rfq, session, trades, rfqs));
        });

        //TODO: publish the metadata
        publisher.publishMetadata(metadata);
    }
}
