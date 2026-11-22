CREATE TABLE order_est_income_for_landlord (
    date                    DATE          NOT NULL,              -- DATE(transaction_date) WIB
    transaction_date        DATETIME      NOT NULL,              -- jam transaksi (WIB)
    outlet_code             VARCHAR(10)   NOT NULL,
    outlet_name             VARCHAR(100)  NULL,
    order_id                VARCHAR(64)   NOT NULL,
    transaction_id          VARCHAR(64)   NOT NULL,
    order_reference         VARCHAR(32)   NULL,                  -- mis. #964-6981
    channel                 VARCHAR(20)   NULL,                  -- pos / web
    payment_type_raw        VARCHAR(100)  NULL,                  -- transactions_payment_type_name asli
    payment_type            VARCHAR(50)   NOT NULL,              -- hasil PAYMENT_TYPE_MAP (label yg dikirim ke landlord)
    total_order_amount      DECIMAL(20,4) NOT NULL DEFAULT 0,    -- gross sebelum diskon/promo
    total_discount_amount   DECIMAL(20,4) NOT NULL DEFAULT 0,
    total_promotion_amount  DECIMAL(20,4) NOT NULL DEFAULT 0,
    subtotal                DECIMAL(20,4) NOT NULL DEFAULT 0,    -- net sales (basis komisi)
    tax_amount              DECIMAL(20,4) NOT NULL DEFAULT 0,
    total_sales             DECIMAL(20,4) NOT NULL DEFAULT 0,    -- total_amount
    commission_pct          DECIMAL(6,4)  NOT NULL DEFAULT 0,    -- rate yg dipakai saat load (audit kalau rate berubah)
    sales_type_commission   DECIMAL(20,4) NOT NULL DEFAULT 0,    -- subtotal * commission_pct, tidak dibulatkan di code
    load_data_at            DATETIME      NULL DEFAULT CURRENT_TIMESTAMP,
    load_data_by            VARCHAR(100)  NULL,
    PRIMARY KEY (order_id, transaction_id),
    KEY idx_date_outlet (date, outlet_code)
);
