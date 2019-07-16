package dev.testdata;

import org.joda.time.DateTime;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class GenerateDummyRfq {
    //input counterparty and instrument files:
    private static final String counterparties_file = "src/test/resources/trades/counterparty-static.csv";
    private static final String instruments_file = "src/test/resources/trades/instrument-static.csv";

    //output trade reports file:
    private static final String rfqs_file = "src/test/resources/trades/rfqs.json";

    //variables:
    private static final int counterparties_limit = 10;
    private static final int instruments_limit = 10;
    private static final int trades_min = 1;
    private static final int trades_max = 15;

    public static void main(String[] args) throws Exception {
//        {
//            'id': 9315444593154445,
//                'traderId': 3351266293154445953,
//                'entityId': 5561279226039690843,
//                'instrumentId': 'AT0000383864',
//                'qty': 250000,
//                'price': 1.58,
//                'side': 'B'
//        }

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

        //generate test data for rfqs
        List<Rfq> rfqs = new ArrayList<>();
        counterparties.forEach(c -> {
            instruments.forEach(i -> {
                    rfqs.add(new Rfq(c, i));
            });
        });


        //order the results by date
//        trades.sort(Comparator.comparing(t -> t.TransactTime));

        //save to rfqs file
        PrintWriter out = new PrintWriter(new FileWriter(Paths.get(rfqs_file).toFile()));
        rfqs.forEach(out::println);
        out.flush();
        out.close();

//        System.out.println("Generated: " + trades.size() + " records");
    }

    /*
     * Generates between min (inclusive) and max (exclusive) random DateTimes over the past year
     */
    private static List<DateTime> rfqDates(int min, int max) {
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
