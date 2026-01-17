-- MySQL schema fixture v1 (multi-tenant ecommerce-ish app)

CREATE TABLE tenants
(
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    slug       VARCHAR(64)  NOT NULL,
    name       VARCHAR(255) NOT NULL,
    plan       ENUM('free','pro','enterprise') NOT NULL DEFAULT 'free',
    settings   JSON NULL,
    created_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_tenants_slug (slug)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='Tenant/account';

CREATE TABLE users
(
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id     BIGINT UNSIGNED NOT NULL,
    email         VARCHAR(255) NOT NULL,
    password_hash VARBINARY(60) NOT NULL,
    display_name  VARCHAR(120) NULL,
    is_active     TINYINT      NOT NULL DEFAULT 1,
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_users_email (email),
    KEY           idx_users_tenant (tenant_id),
    CONSTRAINT fk_users_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT chk_users_email CHECK (email LIKE '%@%')
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='Application user';

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
    user_id    BIGINT UNSIGNED NOT NULL,
    role_id    BIGINT UNSIGNED NOT NULL,
    granted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, role_id),
    KEY        idx_user_roles_role (role_id),
    CONSTRAINT fk_user_roles_user FOREIGN KEY (user_id) REFERENCES users (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT fk_user_roles_role FOREIGN KEY (role_id) REFERENCES roles (id)
        ON DELETE CASCADE ON UPDATE RESTRICT
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
    sku         VARCHAR(64)    NOT NULL,
    name        VARCHAR(255)   NOT NULL,
    description TEXT NULL,
    price       DECIMAL(10, 2) NOT NULL,
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
COLLATE=utf8mb4_unicode_ci
COMMENT='Catalog product'
AUTO_INCREMENT=1000;

CREATE TABLE product_categories
(
    product_id  BIGINT UNSIGNED NOT NULL,
    category_id BIGINT UNSIGNED NOT NULL,
    position    INT NOT NULL DEFAULT 0,
    PRIMARY KEY (product_id, category_id),
    KEY         idx_pc_category (category_id),
    CONSTRAINT fk_pc_product FOREIGN KEY (product_id) REFERENCES products (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT fk_pc_category FOREIGN KEY (category_id) REFERENCES categories (id)
        ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='Product/category membership';

CREATE TABLE orders
(
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id    BIGINT UNSIGNED NOT NULL,
    user_id      BIGINT UNSIGNED NOT NULL,
    order_number VARCHAR(32)    NOT NULL,
    status       ENUM('draft','placed','paid','shipped','cancelled') NOT NULL DEFAULT 'draft',
    total_amount DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    currency     CHAR(3)        NOT NULL DEFAULT 'USD',
    placed_at    TIMESTAMP NULL,
    created_at   TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_orders_tenant_number (tenant_id, order_number),
    KEY          idx_orders_user (user_id),
    KEY          idx_orders_status_created (status, created_at),
    CONSTRAINT fk_orders_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users (id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT chk_orders_total CHECK (total_amount >= 0)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='Customer order';

CREATE TABLE order_items
(
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    order_id   BIGINT UNSIGNED NOT NULL,
    product_id BIGINT UNSIGNED NOT NULL,
    quantity   INT            NOT NULL DEFAULT 1,
    unit_price DECIMAL(10, 2) NOT NULL,
    line_total DECIMAL(10, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    PRIMARY KEY (id),
    UNIQUE KEY uq_order_items_order_product (order_id, product_id),
    KEY        idx_order_items_product (product_id),
    CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES products (id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT chk_order_items_qty CHECK (quantity > 0)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='Order line item';

CREATE TABLE payments
(
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    order_id     BIGINT UNSIGNED NOT NULL,
    provider     ENUM('stripe','paypal','manual') NOT NULL,
    provider_ref VARCHAR(128) NULL,
    amount       DECIMAL(10, 2) NOT NULL,
    currency     CHAR(3)        NOT NULL DEFAULT 'USD',
    status       ENUM('pending','authorized','captured','failed','refunded') NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_payments_provider_ref (provider, provider_ref),
    KEY          idx_payments_order (order_id),
    CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES orders (id)
        ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT chk_payments_amount CHECK (amount >= 0)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='Payment record (v1)';
