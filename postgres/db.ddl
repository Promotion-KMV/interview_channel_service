CREATE TABLE IF NOT EXISTS public.order (
    id uuid PRIMARY KEY,
    num INTEGER NOT NULL,
    orders INTEGER NOT NULL UNIQUE,
    price_usd FLOAT,
    price_rub FLOAT,
    delivery DATE
)