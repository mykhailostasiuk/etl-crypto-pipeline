import os
from pyspark.sql.types import StructField, StringType, LongType, DecimalType, BooleanType, StructType, ArrayType

table_enum = {
    'aggTrade': f"""
        CREATE TABLE IF NOT EXISTS crypto_pipeline.aggTrades (
            first_trade_id UInt64,
            last_trade_id UInt64,
            aggregate_trade_id UInt64,
            timestamp_utc DateTime,
            symbol String,
            price Decimal(14,8),
            quantity Decimal(18,8),
            is_buyer_market_maker Boolean,
        ) ENGINE = MergeTree()
        ORDER BY (symbol, timestamp_utc, aggregate_trade_id);
    """,
    'trade': f"""
        CREATE TABLE IF NOT EXISTS crypto_pipeline.trades (
            trade_id UInt64,
            timestamp_utc DateTime,
            symbol String,
            price Decimal(14,8),
            quantity Decimal(18,8),
            is_buyer_market_maker Boolean,
        ) ENGINE = MergeTree()
        ORDER BY (symbol, timestamp_utc, trade_id);
    """,
    'kline': f"""
        CREATE TABLE IF NOT EXISTS crypto_pipeline.klines (
            first_trade_id UInt64,
            last_trade_id UInt64,
            timestamp_utc DateTime,
            open_timestamp_utc DateTime,
            close_timestamp_utc DateTime,
            symbol String,
            interval String,
            num_trades UInt16,
            is_closed Boolean,   
            open_price Decimal(14,8),
            close_price Decimal(14,8),
            high_price Decimal(14,8),
            low_price Decimal(14,8),
            base_asset_volume Decimal(18,8),
            quote_asset_volume Decimal(18,8),
            taker_buy_base_asset_volume Decimal(18,8),
            taker_buy_quote_asset_volume Decimal(18,8),
        ) ENGINE = ReplacingMergeTree(timestamp_utc)
        ORDER BY (symbol, interval, open_timestamp_utc);
    """,
    'miniTicker': f"""
        CREATE TABLE IF NOT EXISTS crypto_pipeline.miniTickers (
            timestamp_utc DateTime,
            symbol String,
            open_price Decimal(14,8),
            close_price Decimal(14,8),
            high_price Decimal(14,8),
            low_price Decimal(14,8),
            total_base_asset_volume Decimal(20,8),
            total_quote_asset_volume Decimal(20,8),
        ) ENGINE = MergeTree()
        ORDER BY (symbol, timestamp_utc);
    """,
    'ticker': f"""
        CREATE TABLE IF NOT EXISTS crypto_pipeline.tickers (
            first_trade_id UInt64,
            last_trade_id UInt64,
            timestamp_utc DateTime,
            open_timestamp_utc DateTime,
            close_timestamp_utc DateTime,
            symbol String,
            price_change Decimal(14,8),
            price_change_percent Decimal(7,3),
            weighted_avg_price Decimal(14,8),
            num_trades UInt32,
            first_trade_price Decimal(14,8),
            open_price Decimal(14,8),
            close_price Decimal(14,8),
            high_price Decimal(14,8),
            low_price Decimal(14,8),
            best_bid_price Decimal(14,8),
            best_bid_quantity Decimal(18,8),
            best_ask_price Decimal(14,8),
            best_ask_quantity Decimal(18,8),
            last_quantity Decimal(18,8),
            total_base_asset_volume Decimal(20,8),
            total_quote_asset_volume Decimal(20,8),
        ) ENGINE = MergeTree()
        ORDER BY (symbol, timestamp_utc);
    """,
    'avgPrice': f"""
        CREATE TABLE IF NOT EXISTS crypto_pipeline.avgPrices (
            timestamp_utc DateTime,
            last_trade_timestamp_utc DateTime,
            symbol String,
            avg_price_interval String,
            avg_price Decimal(14,8),
        ) ENGINE = MergeTree()
        ORDER BY (symbol, timestamp_utc);
    """,
    'depth': f"""
        CREATE TABLE IF NOT EXISTS crypto_pipeline.depths (
            timestamp_utc DateTime,
            symbol String,
            first_update_id UInt64,
            final_update_id UInt64,
            bids Array(Array(Decimal(18,8))),
            asks Array(Array(Decimal(18,8))),
        ) ENGINE = MergeTree()
        ORDER BY (symbol, timestamp_utc);
    """
}

schema_enum = {
    'aggTrade': StructType([
        StructField('e', StringType(), True),
        StructField('E', LongType(), True),
        StructField('s', StringType(), True),
        StructField('a', LongType(), True),
        StructField('p', DecimalType(14, 8), True),
        StructField('q', DecimalType(18, 8), True),
        StructField('f', LongType(), True),
        StructField('l', LongType(), True),
        StructField('T', LongType(), True),
        StructField('m', BooleanType(), True),
        StructField('M', BooleanType(), True)
    ]),
    'trade': StructType([
        StructField('e', StringType(), True),
        StructField('E', LongType(), True),
        StructField('s', StringType(), True),
        StructField('t', LongType(), True),
        StructField('p', DecimalType(14, 8), True),
        StructField('q', DecimalType(18, 8), True),
        StructField('T', LongType(), True),
        StructField('m', BooleanType(), True),
        StructField('M', BooleanType(), True)
    ]),
    'kline': StructType([
        StructField('e', StringType(), True),
        StructField('E', LongType(), True),
        StructField('s', StringType(), True),
        StructField('k', StructType([
            StructField('t', LongType(), True),
            StructField('T', LongType(), True),
            StructField('s', StringType(), True),
            StructField('i', StringType(), True),
            StructField('f', LongType(), True),
            StructField('L', LongType(), True),
            StructField('o', DecimalType(14, 8), True),
            StructField('c', DecimalType(14, 8), True),
            StructField('h', DecimalType(14, 8), True),
            StructField('l', DecimalType(14, 8), True),
            StructField('v', DecimalType(18, 8), True),
            StructField('n', LongType(), True),
            StructField('x', BooleanType(), True),
            StructField('q', DecimalType(18, 8), True),
            StructField('V', DecimalType(18, 8), True),
            StructField('Q', DecimalType(18, 8), True),
            StructField('B', StringType(), True),
        ]))
    ]),
    'miniTicker': StructType([
        StructField('e', StringType(), True),
        StructField('E', LongType(), True),
        StructField('s', StringType(), True),
        StructField('c', DecimalType(14, 8), True),
        StructField('o', DecimalType(14, 8), True),
        StructField('h', DecimalType(14, 8), True),
        StructField('l', DecimalType(14, 8), True),
        StructField('v', DecimalType(20, 8), True),
        StructField('q', DecimalType(20, 8), True),
    ]),
    'ticker': StructType([
        StructField('e', StringType(), True),
        StructField('E', LongType(), True),
        StructField('s', StringType(), True),
        StructField('p', DecimalType(14, 8), True),
        StructField('P', DecimalType(7, 3), True),
        StructField('w', DecimalType(14, 8), True),
        StructField('x', DecimalType(14, 8), True),
        StructField('c', DecimalType(14, 8), True),
        StructField('Q', DecimalType(18, 8), True),
        StructField('b', DecimalType(14, 8), True),
        StructField('B', DecimalType(18, 8), True),
        StructField('a', DecimalType(14, 8), True),
        StructField('A', DecimalType(18, 8), True),
        StructField('o', DecimalType(14, 8), True),
        StructField('h', DecimalType(14, 8), True),
        StructField('l', DecimalType(14, 8), True),
        StructField('v', DecimalType(20, 8), True),
        StructField('q', DecimalType(20, 8), True),
        StructField('O', LongType(), True),
        StructField('C', LongType(), True),
        StructField('F', LongType(), True),
        StructField('L', LongType(), True),
        StructField('n', LongType(), True),
    ]),
    'avgPrice': StructType([
        StructField('e', StringType(), True),
        StructField('E', LongType(), True),
        StructField('s', StringType(), True),
        StructField('i', StringType(), True),
        StructField('w', DecimalType(14, 8), True),
        StructField('T', LongType(), True),
    ]),
    'depth': StructType([
        StructField("e", StringType(), True),
        StructField("E", LongType(), True),
        StructField("s", StringType(), True),
        StructField("U", LongType(), True),
        StructField("u", LongType(), True),
        StructField("b", ArrayType(ArrayType(DecimalType(18, 8))), True),
        StructField("a", ArrayType(ArrayType(DecimalType(18, 8))), True),
    ])
}


expression_enum = {
    'aggTrade': [
        "f as first_trade_id",
        "l as last_trade_id",
        "a as aggregate_trade_id",
        "E as timestamp_utc",
        "s as symbol",
        "p as price",
        "q as quantity",
        "m as is_buyer_market_maker",
    ],
    'trade': [
        "t as trade_id",
        "E as timestamp_utc",
        "s as symbol",
        "p as price",
        "q as quantity",
        "m as is_buyer_market_maker",
    ],
    'kline': [
        "k.f as first_trade_id",
        "k.L as last_trade_id",
        "E as timestamp_utc",
        "k.t as open_timestamp_utc",
        "k.T as close_timestamp_utc",
        "s as symbol",
        "k.i as interval",
        "k.n as num_trades",
        "k.x as is_closed",
        "k.o as open_price",
        "k.c as close_price",
        "k.h as high_price",
        "k.l as low_price",
        "k.v as base_asset_volume",
        "k.q as quote_asset_volume",
        "k.V as taker_buy_base_asset_volume",
        "k.Q as taker_buy_quote_asset_volume",
    ],
    'miniTicker': [
        "E as timestamp_utc",
        "s as symbol",
        "o as open_price",
        "c as close_price",
        "h as high_price",
        "l as low_price",
        "v as total_base_asset_volume",
        "q as total_quote_asset_volume",
    ],
    'ticker': [
        "F as first_trade_id",
        "L as last_trade_id",
        "E as timestamp_utc",
        "O as open_timestamp_utc",
        "C as close_timestamp_utc",
        "s as symbol",
        "p as price_change",
        "P as price_change_percent",
        "w as weighted_avg_price",
        "n as num_trades",
        "x as first_trade_price",
        "o as open_price",
        "c as close_price",
        "h as high_price",
        "l as low_price",
        "b as best_bid_price",
        "B as best_bid_quantity",
        "a as best_ask_price",
        "A as best_ask_quantity",
        "Q as last_quantity",
        "v as total_base_asset_volume",
        "q as total_quote_asset_volume",
    ],
    'avgPrice': [
        "E as timestamp_utc",
        "T as last_trade_timestamp_utc",
        "s as symbol",
        "i as avg_price_interval",
        "w as avg_price",
    ],
    'depth': [
        "E as timestamp_utc",
        "s as symbol",
        "U as first_update_id",
        "u as final_update_id",
        "b as bids",
        "a as asks",
    ]
}

endpoint_enum = {
        "aggTrade": "aggTrade",
        "trade": "trade",
        "kline": "kline",
        "24hrMiniTicker": "miniTicker",
        "24hrTicker": "ticker",
        "avgPrice": "avgPrice",
        "depthUpdate": "depth",
    }