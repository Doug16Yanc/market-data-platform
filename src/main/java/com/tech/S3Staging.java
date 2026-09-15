package com.tech;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;

/**
 * Helper compartilhado pelos clients de extração (Sidra, Inmet) pra gravar
 * o JSON bruto direto no bucket S3 — Floci local hoje, AWS real depois —
 * em vez de disco. Centralizado aqui pra não duplicar a config de
 * endpoint/credenciais em cada client.
 *
 * Endpoint e credenciais vêm de variáveis de ambiente (ver .env.example na
 * raiz do projeto), não mais hardcoded — mesma fonte usada pelos jobs
 * Spark e pelo init_warehouse.py, pra não ter três lugares divergindo.
 *
 * Este client roda no HOST (você executando main() no IntelliJ), por isso
 * usa FLOCI_ENDPOINT ("localhost:4566"), não FLOCI_ENDPOINT_DOCKER — esse
 * último é só pra serviços dentro da rede do docker-compose.
 *
 * IntelliJ não carrega .env sozinho: instale o plugin "EnvFile" e aponte
 * pro .env na Run Configuration de cada client (SidraClient, InmetClient),
 * ou defina as variáveis manualmente na aba Environment Variables.
 */
final class S3Staging {

    private static final String ENDPOINT = requireEnv("FLOCI_ENDPOINT");
    private static final String ACCESS_KEY = requireEnv("FLOCI_ACCESS_KEY");
    private static final String SECRET_KEY = requireEnv("FLOCI_SECRET_KEY");
    private static final Region REGION = Region.US_EAST_1;

    private static final S3Client CLIENT = S3Client.builder()
            .endpointOverride(URI.create(ENDPOINT))
            .region(REGION)
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
            .serviceConfiguration(S3Configuration.builder()
                    // obrigatório pro Floci (e MinIO-style em geral): sem isso o SDK monta
                    // URL com subdomínio por bucket, que não resolve em storage local
                    .pathStyleAccessEnabled(true)
                    .build())
            .build();

    private S3Staging() {
    }

    static void putJson(String bucket, String key, String json) {
        CLIENT.putObject(
                PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .contentType("application/json")
                        .build(),
                RequestBody.fromString(json));
    }

    /**
     * Falha rápido e com mensagem clara se a variável de ambiente não
     * estiver setada, em vez de deixar o AWS SDK falhar mais tarde com um
     * erro genérico de credenciais (foi exatamente isso que gerou o
     * NoAuthWithAWSException no Spark — aqui a gente evita esse mesmo
     * problema silencioso do lado do client Java).
     */
    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Environment variable " + name + " is not defined — configure the .env "
                            + "(see .env.example) and the EnvFile plugin in the Run Configuration."
            );
        }
        return value;
    }
}