package com.tech;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

/**
 * Minimal client for Twelve Data — extraction only, same pattern as
 * FredClient/StooqClient.
 *
 * Endpoint used: time_series — returns historical OHLCV as JSON for a
 * given symbol. Free tier: 800 calls/day, 8 calls/minute, up to 5000
 * data points per request (outputsize param).
 *
 * Twelve Data signals errors inside a 200 OK body (a "code"/"message"
 * pair), not always via HTTP status — same failure mode as the Alpha
 * Vantage "Information" issue, so that's checked explicitly before
 * staging instead of trusting the status code alone.
 */
public class TwelveClient {

    private static final String BASE_URL = "https://api.twelvedata.com/time_series";
    private static final String BUCKET = System.getenv("FLOCI_BUCKET");
    private static final String BRONZE_PREFIX = "bronze/twelvedata/";
    private static final int OUTPUT_SIZE = 5000;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private final String apiKey;

    public TwelveClient(String apiKey) {
        this.apiKey = apiKey;
    }

    /**
     * Reads the API key from the TWELVEDATA_API_KEY environment variable.
     */
    public static TwelveClient withApiKeyFromEnvironment() {
        String apiKey = System.getenv("TWELVEDATA_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException(
                    "TWELVEDATA_API_KEY environment variable is not set"
            );
        }
        return new TwelveClient(apiKey);
    }

    /**
     * Downloads the full available daily history for a symbol
     * (up to outputsize data points).
     *
     * @param symbol ticker, e.g. "AAPL", "MSFT", "PETR4.SA"
     */
    public String fetchDailySeriesRawJson(String symbol)
            throws IOException, InterruptedException {

        String url = "%s?symbol=%s&interval=1day&outputsize=%d&apikey=%s"
                .formatted(BASE_URL, symbol, OUTPUT_SIZE, apiKey);

        return get(url);
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
                    "Twelve Data responded with status "
                            + response.statusCode()
                            + " for "
                            + url
                            + " — body: "
                            + response.body()
            );
        }

        String body = response.body();

        // Errors come back as 200 OK with {"code": ..., "message": ..., "status": "error"}
        if (body.contains("\"status\":\"error\"")) {
            throw new IOException(
                    "Twelve Data signaled an error for " + url + " — body: " + body
            );
        }

        return body;
    }

    /**
     * Extracts and stages a single symbol's daily series into Bronze.
     */
    public void fetchAndStageSymbol(String symbol) throws IOException, InterruptedException {
        String json = fetchDailySeriesRawJson(symbol);

        S3Staging.putJson(
                BUCKET,
                BRONZE_PREFIX + symbol.toLowerCase() + ".json",
                json
        );
    }

    /**
     * Extracts and stages several symbols. Respects the free tier's
     * 8 calls/minute limit by waiting between requests — with 3
     * symbols today this is defensive more than strictly necessary,
     * but avoids a silent 429 if the symbol list grows.
     */
    public void fetchAndStageSymbolBatch(List<String> symbols) throws InterruptedException {
        for (String symbol : symbols) {
            try {
                fetchAndStageSymbol(symbol);
                System.out.println("OK: " + symbol);
            } catch (IOException e) {
                System.err.println("Failed " + symbol + ": " + e.getMessage());
            }
            Thread.sleep(8_000); // ~7.5 calls/min, margem sob o limite de 8/min
        }
    }

}