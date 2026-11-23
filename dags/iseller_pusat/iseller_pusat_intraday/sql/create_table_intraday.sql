-- Referensi schema (BUKAN migration, dijalankan manual di DBeaver sebelum DAG diaktifkan). Database: data_lake_jiwa.
-- Kolom = sama dengan transactions_iseller_pusat / transactions_items_iseller_pusat (SHOW CREATE TABLE 2026-11-23).
-- Beda: order_id / transaction_id / order_detail_id VARCHAR(64) supaya bisa di-index; ada index order_id + transaction_date;
-- details.transaction_date TANPA "ON UPDATE CURRENT_TIMESTAMP" (jebakan di tabel utama).

CREATE TABLE `transactions_iseller_pusat_intraday` (
  `order_id` varchar(64) DEFAULT NULL,
  `transaction_id` varchar(64) DEFAULT NULL,
  `transaction_date` timestamp NULL DEFAULT NULL,
  `order_reference` text, `outlet_id` text, `outlet_code` text, `outlet_name` text,
  `channel` text, `channel_detail` text,
  `order_date` datetime DEFAULT NULL, `closed_date` datetime DEFAULT NULL,
  `status` text, `payment_status` text, `fulfillment_status` text,
  `transactions_type` text, `transactions_status` text, `transactions_gateway` text,
  `sales_type` text, `transactions_payment_type_name` text, `notes` text,
  `customer_id` text, `customer_first_name` text, `customer_last_name` text,
  `customer_phone_number` text, `customer_email` text, `cashier_id` text,
  `total_order_amount` double DEFAULT NULL, `total_discount_amount` double DEFAULT NULL,
  `total_promotion_amount` double DEFAULT NULL, `subtotal` double DEFAULT NULL,
  `total_tax_amount` double DEFAULT NULL, `total_amount` double DEFAULT NULL,
  `mdr` text,
  `load_data_at` datetime DEFAULT NULL,
  KEY `idx_order_id` (`order_id`),
  KEY `idx_transaction_date` (`transaction_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `transactions_items_iseller_pusat_intraday` (
  `order_id` varchar(64) DEFAULT NULL,
  `order_detail_id` varchar(64) DEFAULT NULL,
  `type` text, `bundling_id` text, `product_id` text, `sku` text,
  `product_name` text, `product_generic_name` text,
  `base_price` double DEFAULT NULL, `quantity` double DEFAULT NULL,
  `total_order_amount` double DEFAULT NULL, `promotion_name` text,
  `promotion_amount` double DEFAULT NULL, `discount_amount` double DEFAULT NULL,
  `subtotal` double DEFAULT NULL,
  `product_variant_id` text, `product_variant_name` text,
  `product_modifier_id` text, `product_modifier_name` text, `notes` text,
  `taxable` tinyint(1) DEFAULT NULL, `tax_percentage` double DEFAULT NULL,
  `tax_amount` double DEFAULT NULL, `additional_charge` double DEFAULT NULL,
  `total_amount` double DEFAULT NULL,
  `load_data_at` datetime DEFAULT NULL,
  `product_type` varchar(255) DEFAULT NULL, `discount_name` varchar(255) DEFAULT NULL,
  `discount_percentage` varchar(255) DEFAULT NULL, `promotion_id` varchar(255) DEFAULT NULL,
  `promotion_type` varchar(255) DEFAULT NULL,
  `transaction_date` timestamp NULL DEFAULT NULL,
  `order_reference` varchar(255) DEFAULT NULL, `outlet_id` varchar(255) DEFAULT NULL,
  `outlet_code` varchar(255) DEFAULT NULL, `outlet_name` varchar(255) DEFAULT NULL,
  `channel` varchar(255) DEFAULT NULL, `channel_detail` varchar(255) DEFAULT NULL,
  `order_date` datetime DEFAULT NULL, `closed_date` datetime DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL, `payment_status` varchar(255) DEFAULT NULL,
  `transactions_type` varchar(255) DEFAULT NULL, `transactions_status` varchar(255) DEFAULT NULL,
  `sales_type` varchar(255) DEFAULT NULL, `transactions_payment_type_name` varchar(255) DEFAULT NULL,
  `customer_id` varchar(255) DEFAULT NULL, `customer_first_name` varchar(255) DEFAULT NULL,
  `customer_last_name` varchar(255) DEFAULT NULL, `customer_phone_number` varchar(255) DEFAULT NULL,
  `customer_email` varchar(255) DEFAULT NULL, `cashier_id` varchar(255) DEFAULT NULL,
  `fulfillment_status` varchar(255) DEFAULT NULL,
  KEY `idx_order_id` (`order_id`),
  KEY `idx_transaction_date` (`transaction_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
