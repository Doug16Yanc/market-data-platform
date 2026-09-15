# Metabase + driver comunitário de DuckDB, seguindo a receita oficial da
# MotherDuck (https://motherduck.com/docs/integrations/bi-tools/metabase/).
#
# Por que build próprio em vez de imagem de terceiro pronta:
# a sudolabs/metabase-duckdb que usávamos antes está sem atualização há anos
# e só publica manifest pra arm64 — não é um problema da nossa config, é a
# imagem mesmo que não é multi-arch. Um .jar Java sobre uma base Debian
# (eclipse-temurin) roda igual em amd64 e arm64, sem esse problema.
#
# IMPORTANTE: a imagem oficial do Metabase é Alpine (musl), e o driver
# DuckDB não roda nela por incompatibilidade de glibc — por isso a base
# aqui é eclipse-temurin (Debian), não a imagem oficial do Metabase.
FROM eclipse-temurin:21-jre

ENV MB_PLUGINS_DIR=/plugins
RUN mkdir -p ${MB_PLUGINS_DIR} /app

# Versões pinadas pra build reproduzível — checar releases antes de atualizar:
# https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases
# (cada release já indica a versão do Metabase com que foi testada)
ADD https://downloads.metabase.com/v0.63.1/metabase.jar /app/metabase.jar
ADD https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/1.5.5.0/duckdb.metabase-driver.jar ${MB_PLUGINS_DIR}/

EXPOSE 3000
CMD ["java", "-jar", "/app/metabase.jar"]