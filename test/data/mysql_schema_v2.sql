-- MySQL schema fixture v2 (major breaking changes vs v1)
-- Highlights:
-- - payments table replaced by payment_transactions
-- - product_categories replaced by product_tags
-- - users email uniqueness becomes per-tenant; user PK switches to BINARY(16)
-- - several FK behaviors change (CASCADE/SET NULL/RESTRICT)
-- - more generated/JSON, changed enums, changed decimals

CREATE TABLE tenants
(
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    slug       VARCHAR(64)  NOT NULL,
    name       VARCHAR(255) NOT NULL,
    plan       ENUM('free','pro','enterprise') NOT NULL DEFAULT 'pro',
    settings   JSON NULL,
    created_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_tenants_slug (slug)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='Tenant/account (v2)';

CREATE TABLE users
(
    id               BINARY(16) NOT NULL,
    tenant_id        BIGINT UNSIGNED NOT NULL,
    email            VARCHAR(320) NOT NULL,
    email_normalized VARCHAR(320) GENERATED ALWAYS AS (LOWER(email)) STORED,
    password_digest  VARBINARY(72) NOT NULL,
    display_name     VARCHAR(160) NULL,
    is_active        TINYINT      NOT NULL DEFAULT 1,
    last_login_at    TIMESTAMP NULL,
    created_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_users_tenant_email (tenant_id, email_normalized),
    KEY              idx_users_tenant_active (tenant_id, is_active),
    CONSTRAINT fk_users_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT chk_users_email CHECK (email LIKE '%@%')
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='Application user (v2, UUID pk)';

CREATE TABLE roles
(
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id   BIGINT UNSIGNED NOT NULL,
    name        VARCHAR(64) NOT NULL,
    description VARCHAR(255) NULL,
    created_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_roles_tenant_name (tenant_id, name),
    CONSTRAINT fk_roles_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='RBAC role';

CREATE TABLE user_roles
(
    user_id    BINARY(16) NOT NULL,
    role_id    BIGINT UNSIGNED NOT NULL,
    granted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    granted_by BINARY(16) NULL,
    PRIMARY KEY (user_id, role_id),
    KEY        idx_user_roles_role (role_id),
    KEY        idx_user_roles_granted_by (granted_by),
    CONSTRAINT fk_user_roles_user FOREIGN KEY (user_id) REFERENCES users (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT fk_user_roles_role FOREIGN KEY (role_id) REFERENCES roles (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT fk_user_roles_granted_by FOREIGN KEY (granted_by) REFERENCES users (id)
        ON DELETE SET NULL ON UPDATE RESTRICT
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='RBAC role assignments';

CREATE TABLE categories
(
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id  BIGINT UNSIGNED NOT NULL,
    parent_id  BIGINT UNSIGNED NULL,
    name       VARCHAR(128) NOT NULL,
    slug       VARCHAR(128) NOT NULL,
    is_hidden  TINYINT      NOT NULL DEFAULT 0,
    created_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_categories_tenant_slug (tenant_id, slug),
    KEY        idx_categories_parent (parent_id),
    CONSTRAINT fk_categories_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT fk_categories_parent FOREIGN KEY (parent_id) REFERENCES categories (id)
        ON DELETE SET NULL ON UPDATE RESTRICT
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='Product categories';

CREATE TABLE products
(
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id   BIGINT UNSIGNED NOT NULL,
    sku         VARCHAR(96)    NOT NULL,
    name        VARCHAR(255)   NOT NULL,
    description MEDIUMTEXT NULL,
    price       DECIMAL(12, 4) NOT NULL,
    currency    CHAR(3)        NOT NULL DEFAULT 'USD',
    attributes  JSON NULL,
    is_active   TINYINT        NOT NULL DEFAULT 1,
    created_at  TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_products_tenant_sku (tenant_id, sku),
    KEY         idx_products_tenant_active (tenant_id, is_active),
    FULLTEXT KEY ft_products_name_desc (name, description),
    CONSTRAINT fk_products_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT chk_products_price CHECK (price >= 0)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci
COMMENT='Catalog product (v2)'
AUTO_INCREMENT=5000;

CREATE TABLE product_tags
(
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id  BIGINT UNSIGNED NOT NULL,
    name       VARCHAR(64) NOT NULL,
    slug       VARCHAR(64) NOT NULL,
    color      VARCHAR(16) NULL,
    created_at TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_tags_tenant_slug (tenant_id, slug),
    CONSTRAINT fk_tags_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='Tags (replaces product_categories)';

CREATE TABLE orders
(
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id    BIGINT UNSIGNED NOT NULL,
    user_id      BINARY(16) NOT NULL,
    order_number VARCHAR(40)    NOT NULL,
    status       ENUM('draft','placed','payment_pending','paid','fulfilled','cancelled') NOT NULL DEFAULT 'draft',
    total_amount DECIMAL(12, 4) NOT NULL DEFAULT 0.0000,
    currency     CHAR(3)        NOT NULL DEFAULT 'USD',
    metadata     JSON NULL,
    placed_at    TIMESTAMP NULL,
    cancelled_at TIMESTAMP NULL,
    created_at   TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_orders_tenant_number (tenant_id, order_number),
    KEY          idx_orders_user (user_id),
    KEY          idx_orders_status_created (status, created_at),
    CONSTRAINT fk_orders_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT chk_orders_total CHECK (total_amount >= 0)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='Customer order (v2)';

CREATE TABLE order_items
(
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    order_id        BIGINT UNSIGNED NOT NULL,
    product_id      BIGINT UNSIGNED NOT NULL,
    quantity        INT            NOT NULL DEFAULT 1,
    unit_price      DECIMAL(12, 4) NOT NULL,
    discount_amount DECIMAL(12, 4) NOT NULL DEFAULT 0.0000,
    line_total      DECIMAL(12, 4) GENERATED ALWAYS AS ((quantity * unit_price) - discount_amount) STORED,
    PRIMARY KEY (id),
    UNIQUE KEY uq_order_items_order_product (order_id, product_id),
    KEY             idx_order_items_product (product_id),
    CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES products (id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT chk_order_items_qty CHECK (quantity > 0),
    CONSTRAINT chk_order_items_discount CHECK (discount_amount >= 0)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='Order line item (v2)';

CREATE TABLE payment_transactions
(
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    order_id     BIGINT UNSIGNED NOT NULL,
    provider     ENUM('stripe','paypal','manual','bank_transfer') NOT NULL,
    provider_ref VARCHAR(191) NULL,
    amount       DECIMAL(12, 4) NOT NULL,
    currency     CHAR(3)        NOT NULL DEFAULT 'USD',
    status       ENUM('pending','authorized','captured','failed','refunded','chargeback') NOT NULL DEFAULT 'pending',
    raw_response JSON NULL,
    created_at   TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_paytx_provider_ref (provider, provider_ref),
    KEY          idx_paytx_order (order_id),
    KEY          idx_paytx_status_created (status, created_at),
    CONSTRAINT fk_paytx_order FOREIGN KEY (order_id) REFERENCES orders (id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT chk_paytx_amount CHECK (amount >= 0)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='Payment transactions (replaces payments)';
