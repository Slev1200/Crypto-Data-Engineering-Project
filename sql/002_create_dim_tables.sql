CREATE TABLE IF NOT EXISTS dim_coin (
    coin_id TEXT PRIMARY KEY,
    symbol TEXT,
    name TEXT,
    sector TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO dim_coin (coin_id, symbol, name, sector)
VALUES
    ('bitcoin', 'btc', 'Bitcoin', 'Store of Value'),
    ('ethereum', 'eth', 'Ethereum', 'Layer 1'),
    ('solana', 'sol', 'Solana', 'Layer 1'),
    ('chainlink', 'link', 'Chainlink', 'Oracle'),
    ('uniswap', 'uni', 'Uniswap', 'DeFi'),
    ('dogecoin', 'doge', 'Dogecoin', 'Meme'),
    ('cardano', 'ada', 'Cardano', 'Layer 1'),
    ('avalanche-2', 'avax', 'Avalanche', 'Layer 1'),
    ('polygon', 'matic', 'Polygon', 'Layer 2'),
    ('litecoin', 'ltc', 'Litecoin', 'Payments')
ON CONFLICT (coin_id) DO UPDATE
SET
    symbol = EXCLUDED.symbol,
    name = EXCLUDED.name,
    sector = EXCLUDED.sector,
    updated_at = NOW();