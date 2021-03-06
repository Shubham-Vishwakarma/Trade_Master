package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import com.cs.rfq.utils.RfqHistorySimulator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.sum;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private final Dataset<Row> trades;

    private final Dataset<Row> rfqs;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final List<RfqMetadataExtractor> extractor2 = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext) {
        this.session = session;
        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives
        this.trades = new TradeDataLoader().loadTrades(session, "src/test/resources/trades/*.json");

        this.rfqs = new RfqDataLoader().loadTrades(session, "src/test/resources/rfqs/*.json");

        //TODO: take a close look at how these two extractors are implemented
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new AverageTradedPriceOverPastWeek());
        extractors.add(new TradeSideBias());
        extractors.add(new InstrumentLiquidityExtractor());
        extractor2.add(new VolumeTradedWithEntityYTDExtractor());
        extractor2.add(new TotalVolumeTradedForInstrumentExtractor());
        extractor2.add(new StrikeRateExtractor());
    }

    public void startSocketListener() throws InterruptedException {
        //TODO: stream data from the input socket on localhost:9000
        JavaDStream<String> stream = streamingContext.socketTextStream("localhost", 9000);

        //TODO: convert each incoming line to a Rfq object and call processRfq method with it
        stream.foreachRDD(rdd -> {
            rdd.collect().stream().map(Rfq::fromJson).forEach(this::processRfq);
        });

        //TODO: start the streaming context
        log.info("Listening for RFQs");
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void processRfq(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        //adding rfqs to log file
        RfqHistorySimulator  rfqhist = new RfqHistorySimulator(rfq);
        rfqhist.logRfq();

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
