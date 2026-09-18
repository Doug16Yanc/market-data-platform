package com.tech;

import java.util.Arrays;
import java.util.List;

/**
 * Roda a extração Bronze completa do market-data-lake: FRED (macro)
 * + Twelve Data (preços). Aciona os clients existentes, não seus mains.
 *
 * Listas de séries/símbolos vêm de env vars (FRED_SERIES, TWELVE_SYMBOLS),
 * comma-separated, pra não precisar recompilar toda vez que mudar o
 * conjunto de ativos monitorados. Ver .env.example.
 */
public class BronzeExtractionRunner {

    private static final List<String> DEFAULT_FRED_SERIES =
            List.of("CPIAUCSL", "FEDFUNDS", "DGS10");

    private static final List<String> DEFAULT_TWELVE_SYMBOLS =
            List.of("AAPL", "MSFT", "GOOGL");

    public static void main(String[] args) throws InterruptedException {
        FredClient fredClient = FredClient.withApiKeyFromEnvironment();
        fredClient.fetchAndStageSeriesBatch(readList("FRED_SERIES", DEFAULT_FRED_SERIES));

        TwelveClient twelveClient = TwelveClient.withApiKeyFromEnvironment();
        twelveClient.fetchAndStageSymbolBatch(readList("TWELVE_SYMBOLS", DEFAULT_TWELVE_SYMBOLS));
    }

    /**
     * Lê uma lista comma-separated de uma env var; cai no default se a
     * variável não estiver definida (pragmático pra não travar quem
     * ainda não configurou o .env com essas chaves).
     */
    private static List<String> readList(String envVar, List<String> defaultValue) {
        String raw = System.getenv(envVar);
        if (raw == null || raw.isBlank()) {
            System.out.println(envVar + " not set, using default: " + defaultValue);
            return defaultValue;
        }
        return Arrays.stream(raw.split(","))
                .map(String::strip)
                .filter(s -> !s.isEmpty())
                .toList();
    }
}