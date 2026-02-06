-- ============================================
-- SADOP - Initialisation de la base
-- Schéma "mini e-commerce" + gros volume de données
-- ============================================

CREATE DATABASE IF NOT EXISTS sadop_db;
USE sadop_db;

-- On nettoie si on relance plusieurs fois
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS sessions;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS users;

-- ============================================
-- 1) Clients
-- ============================================
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    age INT,
    city VARCHAR(100),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ❗ SI tu veux laisser SADOP/ML/RL proposer les index,
-- commente ces lignes. Je les laisse COMMENTÉES exprès.
-- CREATE INDEX idx_users_city ON users(city);
-- CREATE INDEX idx_users_created_at ON users(created_at);


-- ============================================
-- 2) Sessions de navigation / activité
-- ============================================
CREATE TABLE IF NOT EXISTS sessions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    duration INT NOT NULL,           -- en minutes
    session_type VARCHAR(50),        -- 'web', 'mobile', 'cardio', etc.
    device VARCHAR(50),              -- 'desktop', 'android', 'ios', ...
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_sessions_user
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON DELETE CASCADE
);

-- CREATE INDEX idx_sessions_user_id ON sessions(user_id);
-- CREATE INDEX idx_sessions_created_at ON sessions(created_at);
-- CREATE INDEX idx_sessions_session_type ON sessions(session_type);


-- ============================================
-- 3) Produits
-- ============================================
CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10,2) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- CREATE INDEX idx_products_category ON products(category);
-- CREATE INDEX idx_products_price ON products(price);


-- ============================================
-- 4) Commandes (orders)
-- ============================================
CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    status VARCHAR(50) DEFAULT 'PENDING',  -- PENDING, PAID, SHIPPED...
    total_amount DECIMAL(10,2) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_orders_user
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON DELETE CASCADE
);

-- CREATE INDEX idx_orders_user_id ON orders(user_id);
-- CREATE INDEX idx_orders_created_at ON orders(created_at);
-- CREATE INDEX idx_orders_status ON orders(status);


-- ============================================
-- 5) Détail des commandes (order_items)
-- ============================================
CREATE TABLE IF NOT EXISTS order_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    CONSTRAINT fk_items_order
        FOREIGN KEY (order_id) REFERENCES orders(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_items_product
        FOREIGN KEY (product_id) REFERENCES products(id)
        ON DELETE RESTRICT
);

-- CREATE INDEX idx_items_order_id ON order_items(order_id);
-- CREATE INDEX idx_items_product_id ON order_items(product_id);



-- ============================================
-- 6) Remplissage massif (beaucoup de données)
-- ============================================

DELIMITER $$

CREATE PROCEDURE populate_sadop()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE j INT;
    DECLARE k INT;
    DECLARE max_users INT DEFAULT 20000;      -- 20 000 users
    DECLARE sessions_per_user INT;
    DECLARE orders_per_user INT;
    DECLARE items_per_order INT;
    DECLARE product_count INT DEFAULT 1000;   -- 1 000 produits
    DECLARE order_total DECIMAL(10,2);
    DECLARE order_id INT;
    DECLARE product_id INT;
    DECLARE unit_price DECIMAL(10,2);

    -- ============================
    -- 6.1) Produits
    -- ============================
    SET k = 1;
    WHILE k <= product_count DO
        INSERT INTO products (name, category, price, created_at)
        VALUES (
            CONCAT('Product ', k),
            CASE FLOOR(RAND()*5)
                WHEN 0 THEN 'Electronics'
                WHEN 1 THEN 'Books'
                WHEN 2 THEN 'Sports'
                WHEN 3 THEN 'Clothing'
                ELSE 'Food'
            END,
            ROUND(5 + RAND()*195, 2),  -- prix entre 5 et 200 €
            NOW() - INTERVAL FLOOR(RAND()*365) DAY
        );
        SET k = k + 1;
    END WHILE;

    -- ============================
    -- 6.2) Users + Sessions + Orders + Order_items
    -- ============================
    WHILE i <= max_users DO
        -- USER
        INSERT INTO users (name, email, age, city, created_at)
        VALUES (
            CONCAT('User ', i),
            CONCAT('user', i, '@example.com'),
            18 + FLOOR(RAND()*50),             -- âge 18–67
            CASE FLOOR(RAND()*8)
                WHEN 0 THEN 'Paris'
                WHEN 1 THEN 'Lyon'
                WHEN 2 THEN 'Marseille'
                WHEN 3 THEN 'Lille'
                WHEN 4 THEN 'Nice'
                WHEN 5 THEN 'Toulouse'
                WHEN 6 THEN 'Mulhouse'
                ELSE 'Strasbourg'
            END,
            NOW() - INTERVAL FLOOR(RAND()*365) DAY
        );

        -- SESSIONS
        SET sessions_per_user = 1 + FLOOR(RAND()*5);  -- 1 à 5 sessions
        SET j = 1;
        WHILE j <= sessions_per_user DO
            INSERT INTO sessions (user_id, duration, session_type, device, created_at)
            VALUES (
                i,
                5 + FLOOR(RAND()*115), -- 5–120 minutes
                CASE FLOOR(RAND()*3)
                    WHEN 0 THEN 'web'
                    WHEN 1 THEN 'mobile'
                    ELSE 'api'
                END,
                CASE FLOOR(RAND()*3)
                    WHEN 0 THEN 'desktop'
                    WHEN 1 THEN 'android'
                    ELSE 'ios'
                END,
                NOW() - INTERVAL FLOOR(RAND()*60) DAY
            );
            SET j = j + 1;
        END WHILE;

        -- ORDERS
        SET orders_per_user = FLOOR(RAND()*4);   -- 0 à 3 commandes
        SET j = 1;
        WHILE j <= orders_per_user DO
            SET order_total = 0.0;

            INSERT INTO orders (user_id, status, total_amount, created_at)
            VALUES (
                i,
                CASE FLOOR(RAND()*3)
                    WHEN 0 THEN 'PENDING'
                    WHEN 1 THEN 'PAID'
                    ELSE 'SHIPPED'
                END,
                0.0, -- on mettra à jour après
                NOW() - INTERVAL FLOOR(RAND()*60) DAY
            );

            SET order_id = LAST_INSERT_ID();

            -- ITEMS pour cette commande
            SET items_per_order = 1 + FLOOR(RAND()*4);  -- 1 à 4 items
            SET k = 1;
            WHILE k <= items_per_order DO
                SET product_id = 1 + FLOOR(RAND()*product_count);

                -- on récupère le prix du produit
                SELECT price INTO unit_price FROM products WHERE id = product_id LIMIT 1;

                INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                VALUES (
                    order_id,
                    product_id,
                    1 + FLOOR(RAND()*3),  -- quantité 1–4
                    unit_price
                );

                SET order_total = order_total + (unit_price * (1 + FLOOR(RAND()*3)));

                SET k = k + 1;
            END WHILE;

            -- mise à jour du total
            UPDATE orders
            SET total_amount = order_total
            WHERE id = order_id;

            SET j = j + 1;
        END WHILE;

        SET i = i + 1;
    END WHILE;
END$$

DELIMITER ;

-- Lancer le remplissage
CALL populate_sadop();

-- Optionnel : on supprime la procédure après coup
DROP PROCEDURE IF EXISTS populate_sadop;
