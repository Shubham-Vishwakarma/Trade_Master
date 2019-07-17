package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TradeSideBias implements RfqMetadataExtractor{

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd");
        LocalDate localDate = LocalDate.now();
        LocalDate pastWeek = localDate.minusWeeks(1);
        LocalDate pastMonth = localDate.minusMonths(1);

        String queryForWeek = String.format("SELECT Side from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                pastWeek);

        String queryForMonth = String.format("SELECT Side from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                pastMonth);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryForWeekResults = session.sql(queryForWeek);
        Dataset<Row> sqlQueryForMonthResults = session.sql(queryForMonth);


        Object buyForWeek = sqlQueryForWeekResults.filter(trades.col("Side").equalTo(1)).count();
        Object sellForWeek = sqlQueryForWeekResults.filter(trades.col("Side").equalTo(2)).count();

        String buyUponSellRatioForWeek = null;
        if((long)buyForWeek == 0 && (long)sellForWeek == 0) {
            buyUponSellRatioForWeek = new Ratio(-1, 1).RatioToString();
        } else {
            buyUponSellRatioForWeek = new Ratio((long) buyForWeek, (long) sellForWeek).RatioToString();
        }

        Object buyForMonth = sqlQueryForMonthResults.filter(trades.col("Side").equalTo(1)).count();
        Object sellForMonth = sqlQueryForMonthResults.filter(trades.col("Side").equalTo(2)).count();

        String buyUponSellRatioForMonth = null;
        if((long)buyForMonth == 0 && (long)sellForMonth == 0) {
            buyUponSellRatioForMonth = new Ratio(-1, 1).RatioToString();
        } else {
            buyUponSellRatioForMonth = new Ratio((long)buyForMonth, (long)sellForMonth).RatioToString();
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.buyUponSellForWeek, buyUponSellRatioForWeek);
        results.put(RfqMetadataFieldNames.buyUponSellForMonth, buyUponSellRatioForMonth);
        return results;
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades, Dataset<Row> rfqs) {
        return null;
    }


}
