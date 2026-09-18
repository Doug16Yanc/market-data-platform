#!/usr/bin/env python3
"""
Cria/atualiza o arquivo DuckDB com views sobre as camadas Silver e Gold (Delta)
armazenadas no Floci (S3 local).

Usado pelo serviço duckdb-init do docker-compose.
"""

import os
import duckdb

WAREHOUSE_PATH = "/data/warehouse/warehouse.duckdb"
BUCKET = os.getenv("FLOCI_BUCKET", "market-data-lake")
ENDPOINT = os.getenv("FLOCI_ENDPOINT_DOCKER", "http://floci:4566")
ACCESS_KEY = os.getenv("FLOCI_ACCESS_KEY", "test")
SECRET_KEY = os.getenv("FLOCI_SECRET_KEY", "test")

# Symbols tratados como ETF (o resto é considerado ação individual).
# Mantido aqui, num só lugar, pra evitar repetir o CASE em várias views.
ETF_SYMBOLS = ("QQQ", "XLK", "VGT", "SOXX")

# ETF usado como referência de setor pro cálculo de alpha em
# gold_sector_comparison. Trocar aqui se quiser comparar contra outro
# benchmark (ex.: QQQ em vez de XLK).
SECTOR_BENCHMARK_SYMBOL = "XLK"


def main():
    os.makedirs(os.path.dirname(WAREHOUSE_PATH), exist_ok=True)

    con = duckdb.connect(WAREHOUSE_PATH)

    # Extensões necessárias
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL delta; LOAD delta;")

    # Configuração S3 / Floci
    endpoint_host = ENDPOINT.replace("http://", "").replace("https://", "")
    con.execute(f"""
        CREATE OR REPLACE PERSISTENT SECRET floci_s3 (
            TYPE S3,
            KEY_ID '{ACCESS_KEY}',
            SECRET '{SECRET_KEY}',
            ENDPOINT '{endpoint_host}',
            URL_STYLE 'path',
            USE_SSL false
        );
    """)

    # -------------------------------------------------
    # Views Silver
    # -------------------------------------------------
    con.execute(f"""
        CREATE OR REPLACE VIEW silver_price_with_macro AS
        SELECT * FROM delta_scan('s3://{BUCKET}/silver/price_with_macro');
    """)

    # -------------------------------------------------
    # Views Gold (raw, 1:1 com as tabelas Delta)
    # -------------------------------------------------
    con.execute(f"""
        CREATE OR REPLACE VIEW gold_daily_features AS
        SELECT * FROM delta_scan('s3://{BUCKET}/gold/daily_features');
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW gold_symbol_summary AS
        SELECT * FROM delta_scan('s3://{BUCKET}/gold/symbol_summary');
    """)

    # -------------------------------------------------
    # View útil para o Metabase (últimos 2 anos + features principais),
    # já enriquecida com asset_type/sector pra não repetir o CASE em
    # toda view que precisar dessa classificação.
    # -------------------------------------------------
    etf_list_sql = ", ".join(f"'{s}'" for s in ETF_SYMBOLS)

    con.execute(f"""
        CREATE OR REPLACE VIEW gold_daily_recent AS
        SELECT
            symbol,
            CASE
                WHEN symbol IN ({etf_list_sql}) THEN 'ETF'
                ELSE 'STOCK'
            END AS asset_type,
            CASE
                WHEN symbol = 'SOXX' THEN 'Semiconductors'
                WHEN symbol IN ('QQQ', 'XLK', 'VGT') THEN 'Tech (broad)'
                WHEN symbol IN ('NVDA', 'AMD', 'AVGO') THEN 'Semiconductors'
                ELSE 'Tech (broad)'
            END AS sector,
            date,
            close,
            volume,
            return_1d,
            return_21d,
            vol_ann_21d,
            rsi_14,
            dist_sma_50,
            dist_sma_200,
            drawdown_252d,
            cpi,
            fedFundsRate,
            treasury10y
        FROM gold_daily_features
        WHERE date >= current_date - INTERVAL 2 YEAR;
    """)

    # -------------------------------------------------
    # View: comparação setorial (ações vs ETF de referência do setor)
    # Traz, lado a lado, o retorno do ativo e o retorno do ETF
    # benchmark no mesmo dia, já com o alpha (diferença) calculado.
    # -------------------------------------------------
    con.execute(f"""
        CREATE OR REPLACE VIEW gold_sector_comparison AS
        WITH sector_etf AS (
            SELECT
                date,
                return_1d  AS sector_return_1d,
                return_21d AS sector_return_21d
            FROM gold_daily_recent
            WHERE symbol = '{SECTOR_BENCHMARK_SYMBOL}'
        )
        SELECT
            c.symbol,
            c.asset_type,
            c.sector,
            c.date,
            c.close,
            c.return_1d,
            c.return_21d,
            c.vol_ann_21d,
            c.rsi_14,
            c.dist_sma_50,
            c.dist_sma_200,
            c.drawdown_252d,
            e.sector_return_1d,
            e.sector_return_21d,
            c.return_1d  - e.sector_return_1d  AS alpha_1d,
            c.return_21d - e.sector_return_21d AS alpha_21d,
            c.cpi,
            c.fedFundsRate,
            c.treasury10y
        FROM gold_daily_recent c
        LEFT JOIN sector_etf e ON c.date = e.date
        ORDER BY c.date DESC, c.symbol;
    """)

    # -------------------------------------------------
    # View: ranking atual (leaderboard, 1 linha por symbol)
    # -------------------------------------------------
    con.execute(f"""
        CREATE OR REPLACE VIEW gold_leaderboard AS
        SELECT
            symbol,
            CASE
                WHEN symbol IN ({etf_list_sql}) THEN 'ETF'
                ELSE 'STOCK'
            END AS asset_type,
            as_of_date,
            last_close,
            return_1m,
            return_3m,
            vol_ann_1m,
            rsi_14,
            drawdown_252d,
            dist_sma_50,
            dist_sma_200,
            RANK() OVER (ORDER BY return_1m DESC) AS rank_return_1m,
            RANK() OVER (ORDER BY vol_ann_1m ASC)  AS rank_lowest_vol
        FROM gold_symbol_summary
        ORDER BY return_1m DESC;
    """)

    print("Warehouse DuckDB atualizado com sucesso:")
    print("  - silver_price_with_macro")
    print("  - gold_daily_features")
    print("  - gold_symbol_summary")
    print("  - gold_daily_recent")
    print("  - gold_sector_comparison")
    print("  - gold_leaderboard")

    con.close()


if __name__ == "__main__":
    main()