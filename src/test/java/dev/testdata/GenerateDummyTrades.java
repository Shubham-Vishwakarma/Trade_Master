package dev.testdata;

import org.joda.time.DateTime;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility program to generate test trade data.
 *
 * Give two input sets (counterparty and instrument), an ordered
 * pair of counterparty x instrument is taken form the sets
 * cartesian product. For each ordered pair, a set of n trades is
 * generated in the output set. The size of n can be random, its
 * min and max size is determined in the variables section, along
 * with size limits for the input sets.
 */
public class GenerateDummyTrades {

    //input counterparty and instrument files:
    private static final String counterparties_file = "src/test/resources/trades/counterparty-static.csv";
    private static final String instruments_file = "src/test/resources/trades/instrument-static.csv";

    //output trade reports file:
    private static final String trades_file = "src/test/resources/trades/trades.json";

    //variables:
    private static final int counterparties_limit = 10;
    private static final int instruments_limit = 10;
    private static final int trades_min = 1;
    private static final int trades_max = 15;

    public static void main(String[] args) throws Exception {
        //to test reading of json file
//        Stream<String> rfqs = Files.lines(Paths.get(rfqs_file));
//        rfqs.forEach(r->{
//            String[] fields = r.split(",");
//            //System.out.println(fields[0]);
//
//        });



        //load counterparty data
        Set<Counterparty> counterparties = Files.lines(Paths.get(counterparties_file))
                .filter(line -> !line.startsWith("traderId"))
                .limit(counterparties_limit)
                .map(Counterparty::fromCsv)
                .collect(Collectors.toSet());

        //load instrument data
        Set<Instrument> instruments = Files.lines(Paths.get(instruments_file))
                .filter(Instrument::validateCsv)
                .limit(instruments_limit)
                .map(Instrument::fromCsv)
                .collect(Collectors.toSet());

        Random r = new Random();
        //generate test data for trades
        List<TradeCaptureReport> trades = new ArrayList<>();
        counterparties.forEach(c -> {
            instruments.forEach(i -> {
                long o = Math.abs(r.nextLong());
                tradeDates(trades_min, trades_max).forEach(tradeDate -> {
                    trades.add(new TradeCaptureReport(c, i, tradeDate,1.3,2L,1,67L));
                });
            });
        });

        //order the results by date
        trades.sort(Comparator.comparing(t -> t.TransactTime));

        //save to trades file
        PrintWriter out = new PrintWriter(new FileWriter(Paths.get(trades_file).toFile()));
        trades.forEach(out::println);
        out.flush();
        out.close();

        System.out.println("Generated: " + trades.size() + " records");
    }

    /*
     * Generates between min (inclusive) and max (exclusive) random DateTimes over the past year
     */
    private static List<DateTime> tradeDates(int min, int max) {
        int num = min + (int) (Math.random() * (max - min));
        List<DateTime> results = new ArrayList<>();

        final long msInYear = 365 * 24 * 60 * 60 * 1000L;
        final long msNow = System.currentTimeMillis();

        for (int i = 0; i < num; i++) {
            long tradeTimeMs = msNow - (long) (Math.random() * msInYear);
            DateTime tradeTime = new DateTime(tradeTimeMs);
            results.add(tradeTime);
        }

        return results;
    }

}
