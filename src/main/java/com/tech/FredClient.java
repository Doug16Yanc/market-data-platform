package com.tech;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

/**
 * Minimal client for the FRED (Federal Reserve Economic Data) API —
 * extraction only, decoupled from the MacroeconomicsClient in the
 * macroeconomics-ai project (that one is a Spring bean with
 * @Cacheable, built to feed the agent; this one is just the data
 * lake's extraction client, following the same pattern as
 * AlphaVantageClient/NasaPowerClient).
 *
 * Endpoint used: series/observations — returns the full historical
 * series for an indicator (e.g. CPIAUCSL, FEDFUNDS).
 *
 * No aggressively documented rate limit for normal use (much more
 * relaxed than Alpha Vantage), but it's still not a good idea to fire
 * off dozens of series in parallel without a reason to.
 */
public class FredClient {

    private static final String BASE_URL =
            "https://api.stlouisfed.org/fred/series/observations";

    private static final String BUCKET = "market-data-lake";
    private static final String BRONZE_PREFIX = "bronze/fred/";

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private final String apiKey;

    public FredClient(String apiKey) {
        this.apiKey = apiKey;
    }

    /**
     * Reads the API key from the FRED_API_KEY environment variable.
     */
    public static FredClient withApiKeyFromEnvironment() {
        String apiKey = System.getenv("FRED_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException(
                    "FRED_API_KEY environment variable is not set"
            );
        }
        return new FredClient(apiKey);
    }

    /**
     * Downloads the full historical series for a FRED indicator.
     *
     * @param seriesId series code, e.g. "CPIAUCSL", "FEDFUNDS", "DGS10"
     */
    public String fetchSeriesRawJson(String seriesId)
            throws IOException, InterruptedException {

        String url = "%s?series_id=%s&api_key=%s&file_type=json"
                .formatted(BASE_URL, seriesId, apiKey);

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
                    "FRED responded with status "
                            + response.statusCode()
                            + " for "
                            + url
                            + " — body: "
                            + response.body()
            );
        }

        return response.body();
    }

    /**
     * Extracts and stages a single indicator's series into Bronze.
     */
    public void fetchAndStageSeries(String seriesId)
            throws IOException, InterruptedException {

        String json = fetchSeriesRawJson(seriesId);

        S3Staging.putJson(
                BUCKET,
                BRONZE_PREFIX + seriesId + ".json",
                json
        );
    }

    /**
     * Extracts and stages several series. No delay between calls —
     * FRED doesn't have the same tight rate limit as Alpha Vantage —
     * but if an indicator fails, it's logged and the batch continues.
     */
    public void fetchAndStageSeriesBatch(List<String> seriesIds)
            throws InterruptedException {

        for (String seriesId : seriesIds) {
            try {
                fetchAndStageSeries(seriesId);
                System.out.println("OK: " + seriesId);
            } catch (IOException e) {
                System.err.println("Failed " + seriesId + ": " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        FredClient client = FredClient.withApiKeyFromEnvironment();

        // Reuses the same indicators already validated in
        // macroeconomics-ai as a starting point.
        List<String> series = List.of("CPIAUCSL", "FEDFUNDS", "DGS10");

        client.fetchAndStageSeriesBatch(series);
    }
}