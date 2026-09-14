CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,

    email VARCHAR(255) UNIQUE,
    phone_number VARCHAR(20),

    date_of_birth DATE,

    status VARCHAR(20) NOT NULL
        CHECK (status IN ('ACTIVE', 'INACTIVE', 'SUSPENDED')),

    segment VARCHAR(20) NOT NULL
        CHECK (segment IN ('STANDARD', 'GOLD', 'PREMIUM')),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT customer_contact_required
        CHECK (
            email IS NOT NULL
            OR phone_number IS NOT NULL
        )
);

CREATE TABLE IF NOT EXISTS customer_addresses (
    address_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    customer_id BIGINT NOT NULL,

    address_type VARCHAR(20) NOT NULL
        CHECK (address_type IN ('HOME', 'SHIPPING', 'BILLING')),

    address_line_1 VARCHAR(255) NOT NULL,
    address_line_2 VARCHAR(255),

    city VARCHAR(100) NOT NULL,
    state VARCHAR(100),
    postal_code VARCHAR(20) NOT NULL,
    country VARCHAR(100) NOT NULL,

    is_primary BOOLEAN NOT NULL DEFAULT FALSE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_customer_address
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS customer_consents (
    consent_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    customer_id BIGINT NOT NULL,

    consent_type VARCHAR(20) NOT NULL
        CHECK (consent_type IN ('EMAIL', 'SMS', 'MARKETING')),

    granted BOOLEAN NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_customer_consent
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id)
        ON DELETE CASCADE,

    CONSTRAINT uq_customer_consent
        UNIQUE (customer_id, consent_type)
);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE ON customers
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_customer_addresses_updated_at
BEFORE UPDATE ON customer_addresses
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_customer_consents_updated_at
BEFORE UPDATE ON customer_consents
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE ROLE debezium
WITH
    LOGIN
    REPLICATION
    PASSWORD 'debezium_local';

GRANT CONNECT ON DATABASE atlas TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON TABLE
    customers,
    customer_addresses,
    customer_consents
TO debezium;

CREATE PUBLICATION atlas_customer_publication
FOR TABLE
    customers,
    customer_addresses,
    customer_consents;

ALTER TABLE customers
REPLICA IDENTITY FULL;

ALTER TABLE customer_addresses
REPLICA IDENTITY FULL;

ALTER TABLE customer_consents
REPLICA IDENTITY FULL;