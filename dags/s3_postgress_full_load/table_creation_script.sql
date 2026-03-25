CREATE SCHEMA IF NOT EXISTS dwh AUTHORIZATION warehouse_user;

CREATE TABLE IF NOT EXISTS dwh.users (
    user_id BIGINT PRIMARY KEY,
    user_phone TEXT
);

CREATE TABLE IF NOT EXISTS dwh.drivers (
    driver_id BIGINT PRIMARY KEY,
    driver_phone TEXT
);

CREATE TABLE IF NOT EXISTS dwh.stores (
    store_id BIGINT PRIMARY KEY,
    store_address TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.products (
    item_id BIGINT PRIMARY KEY,
    item_title TEXT NOT NULL,
    item_category TEXT
);

CREATE TABLE IF NOT EXISTS dwh.orders (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    store_id BIGINT NOT NULL,
    address_text TEXT,
    created_at TIMESTAMP NOT NULL,
    paid_at TIMESTAMP,
    canceled_at TIMESTAMP,
    payment_type TEXT,
    order_discount INT,
    order_cancellation_reason TEXT,
    delivery_cost NUMERIC(12, 2),
    CONSTRAINT fk_orders_user
        FOREIGN KEY (user_id) REFERENCES dwh.users(user_id),
    CONSTRAINT fk_orders_store
        FOREIGN KEY (store_id) REFERENCES dwh.stores(store_id)
);

CREATE TABLE IF NOT EXISTS dwh.order_items (
    order_item_id TEXT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    item_id BIGINT NOT NULL,
    item_quantity INT,
    item_price NUMERIC(12, 2),
    item_canceled_quantity INT,
    item_discount INT,
    item_replaced_id BIGINT,
    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id) REFERENCES dwh.orders(order_id),
    CONSTRAINT fk_order_items_product
        FOREIGN KEY (item_id) REFERENCES dwh.products(item_id),
    CONSTRAINT fk_order_items_replaced_product
        FOREIGN KEY (item_replaced_id) REFERENCES dwh.products(item_id)
);

CREATE TABLE IF NOT EXISTS dwh.order_driver_assignments (
    assignment_id TEXT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    driver_id BIGINT NOT NULL,
    delivery_started_at TIMESTAMP,
    delivered_at TIMESTAMP,
    CONSTRAINT fk_assignments_order
        FOREIGN KEY (order_id) REFERENCES dwh.orders(order_id),
    CONSTRAINT fk_assignments_driver
        FOREIGN KEY (driver_id) REFERENCES dwh.drivers(driver_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id
    ON dwh.orders(user_id);

CREATE INDEX IF NOT EXISTS idx_orders_store_id
    ON dwh.orders(store_id);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id
    ON dwh.order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_order_items_item_id
    ON dwh.order_items(item_id);

CREATE INDEX IF NOT EXISTS idx_order_driver_assignments_order_id
    ON dwh.order_driver_assignments(order_id);

CREATE INDEX IF NOT EXISTS idx_order_driver_assignments_driver_id
    ON dwh.order_driver_assignments(driver_id);