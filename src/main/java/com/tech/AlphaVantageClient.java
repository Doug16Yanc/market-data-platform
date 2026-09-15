package com.tech;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

/**
 * Minimal client for Alpha Vantage — same philosophy as the other
 * clients in the pipeline: it only EXTRACTS the raw JSON and writes it
 * to the Bronze layer. No parsing/transformation happens here.
 *
 * Endpoint used: TIME_SERIES_DAILY (daily price series;
 * outputsize=full returns the entire historical series, not just the
 * last 100 points).
 *
 * IMPORTANT — free tier rate limit:
 * 5 requests per minute, 25 per day (double-check your plan's current
 * quota — Alpha Vantage has changed these limits before). That's why
 * fetchAndStageDailyBatch() spaces calls out with a safety interval
 * instead of firing everything at once — unlike INMET, this rate
 * limit is documented and predictable, so it can be handled
 * deterministically.
 */
public class AlphaVantageClient {

    private static final String BASE_URL = "https://www.alphavantage.co/query";

    private static final String BUCKET = "market-data-lake";
    private static final String BRONZE_PREFIX = "bronze/alpha_vantage/";

    // 5 req/min on the free tier = 1 every 12s; using 13s as a safety
    // margin instead of cutting it exactly at the limit.
    private static final long DELAY_BETWEEN_CALLS_MS = 13_000;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private final String apiKey;

    public AlphaVantageClient(String apiKey) {
        this.apiKey = apiKey;
    }

    /**
     * Reads the API key from the ALPHA_VANTAGE_API_KEY environment
     * variable.
     */
    public static AlphaVantageClient withApiKeyFromEnvironment() {
        String apiKey = System.getenv("ALPHA_VANTAGE_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException(
                    "ALPHA_VANTAGE_API_KEY environment variable is not set"
            );
        }
        return new AlphaVantageClient(apiKey);
    }

    /**
     * Downloads the full daily series (outputsize=full) for a ticker.
     *
     * @param symbol ticker, e.g. "AAPL", "PETR4.SAO" (the .SAO suffix
     *               is for B3 — confirm ticker coverage for Brazilian
     *               stocks on your plan before relying on it in
     *               production)
     */
    public String fetchDailyRawJson(String symbol)
            throws IOException, InterruptedException {

        String url = "%s?function=TIME_SERIES_DAILY&symbol=%s&outputsize=full&apikey=%s"
                .formatted(BASE_URL, symbol, apiKey);

        String json = get(url);

        // Alpha Vantage returns HTTP 200 even when the request limit
        // has been hit or the symbol is invalid — the error comes
        // inside the body instead of the status code. Needs an
        // explicit check.
        if (json.contains("\"Note\"") || json.contains("\"Information\"")) {
            throw new IOException(
                    "Alpha Vantage signaled a rate limit or warning for "
                            + symbol
                            + " — body: "
                            + json
            );
        }
        if (json.contains("\"Error Message\"")) {
            throw new IOException(
                    "Alpha Vantage returned an error for symbol "
                            + symbol
                            + " — body: "
                            + json
            );
        }

        return json;
    }

    private String get(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException(
                    "Alpha Vantage responded with status "
                            + response.statusCode()
                            + " for "
                            + url
            );
        }

        return response.body();
    }

    /**
     * Extracts and stages the daily series for a single symbol into
     * Bronze.
     */
    public void fetchAndStageDaily(String symbol)
            throws IOException, InterruptedException {

        String json = fetchDailyRawJson(symbol);

        S3Staging.putJson(
                BUCKET,
                BRONZE_PREFIX + symbol + "_daily.json",
                json
        );
    }

    /**
     * Extracts and stages the daily series for several symbols,
     * respecting the free tier rate limit (spaces calls out by
     * DELAY_BETWEEN_CALLS_MS). If a symbol fails, it's logged and the
     * batch continues instead of aborting entirely.
     */
    public void fetchAndStageDailyBatch(List<String> symbols)
            throws InterruptedException {

        for (int i = 0; i < symbols.size(); i++) {
            String symbol = symbols.get(i);
            try {
                fetchAndStageDaily(symbol);
                System.out.println("OK: " + symbol);
            } catch (IOException e) {
                System.err.println("Failed " + symbol + ": " + e.getMessage());
            }

            boolean lastInList = (i == symbols.size() - 1);
            if (!lastInList) {
                Thread.sleep(DELAY_BETWEEN_CALLS_MS);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        AlphaVantageClient client = AlphaVantageClient.withApiKeyFromEnvironment();

        List<String> symbols = List.of("AAPL", "MSFT", "PETR4.SAO");

        client.fetchAndStageDailyBatch(symbols);
    }
}