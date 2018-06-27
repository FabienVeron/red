package io.fab.red;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

class Util {
    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }
}

@Builder
class Stock {
    String code;
    String name;
    BigDecimal initialClosingPrice;

    Map<Date, BigDecimal> closingPrices;
}

@Service
@Slf4j
class StocksService {
    private final ClassPathResource CSV_STOCKS = new ClassPathResource("con_21Jun18.csv");

    private List<Stock> stocks = new ArrayList<>();

    List<Stock> getStocks() {
        return new ArrayList<>(stocks);
    }

    void initStocks() {
        try {
            InputStream inputStream = CSV_STOCKS.getInputStream();
            CSVParser parser = CSVParser.parse(inputStream, Charset.forName("UTF-16"), CSVFormat.TDF.withQuote('"'));
            for (CSVRecord record : parser) {
                Stock stock = Stock.builder().code(record.get(2)).name(record.get(3))
                        .initialClosingPrice(new BigDecimal(record.get(8)))
                        .closingPrices(new HashMap<>())
                        .build();
                stock.closingPrices = generateClosingPrices(stock);
                stocks.add(stock);
            }
        } catch (Exception e) {
            log.error("Could not read CSV file", e);
        }
    }

    @Cacheable(value = "closingPrice")
    public BigDecimal getClosingPrice(Date date) {
        Util.sleep(5);
        return stocks.stream().filter(s -> "0267.HK".equals(s.code)).findFirst().get().closingPrices.get(date);
    }

    @Cacheable(value = "prices")
    public List<BigDecimal> getClosingPrices(String code) {
        Util.sleep(400);
        return new ArrayList<>(stocks.stream().filter(s -> s.code.equals(code)).findFirst().get().closingPrices.values());
    }

    private Map<Date, BigDecimal> generateClosingPrices(Stock stock) {
        Map<Date, BigDecimal> closingPrices = new HashMap<>();
        BigDecimal initialPrice = stock.initialClosingPrice;
        for (int i = 0; i < 365 * 10; i++) {
            Date dayBefore = Date.from(LocalDate.now().minusDays(i).atStartOfDay(ZoneId.systemDefault()).toInstant());
            BigDecimal previousPrice = generatePrice(initialPrice);
            closingPrices.put(dayBefore, previousPrice);
            initialPrice = previousPrice;
        }
        return closingPrices;
    }

    private BigDecimal generatePrice(BigDecimal initialPrice) {
        double move = (Math.random() - 0.5) / 10;
        double newPrice = initialPrice.doubleValue() * (1 + move);
        return new BigDecimal(newPrice).setScale(2, RoundingMode.CEILING);
    }
}

@Component
class AppRunner implements ApplicationRunner {

    @Autowired
    StocksService stocksService;

    double computeAverage(String code) {
        List<BigDecimal> closingPrices = stocksService.getClosingPrices(code);
        double acc = 0;
        for (BigDecimal price : closingPrices) {
            acc += price.doubleValue();
        }
        return acc / closingPrices.size();
    }

    void displayAverages() {
        for (Stock stock : stocksService.getStocks()) {
            System.out.println("stock.code = " + stock.code);
            System.out.println("average = " + computeAverage(stock.code));
        }
    }

    @Override
    public void run(ApplicationArguments args) {
        stocksService.initStocks();
        displayAverages();
    }
}

@SpringBootApplication
@EnableCaching
public class RedApplication {
    public static void main(String[] args) {
        SpringApplication.run(RedApplication.class, args);
    }
}
