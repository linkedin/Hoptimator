-- Create test database and tables for Hoptimator MySQL integration
-- This script runs automatically when the MySQL container starts

USE testdb;

-- Create a users table
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Create an orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending',
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Create a products table
CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INT DEFAULT 0,
    description TEXT
);

-- Insert sample data into users
INSERT INTO users (username, email, is_active) VALUES
    ('alice', 'alice@example.com', TRUE),
    ('bob', 'bob@example.com', TRUE),
    ('charlie', 'charlie@example.com', FALSE),
    ('diana', 'diana@example.com', TRUE);

-- Insert sample data into products
INSERT INTO products (product_name, category, price, stock_quantity, description) VALUES
    ('Laptop', 'Electronics', 999.99, 50, 'High-performance laptop'),
    ('Mouse', 'Electronics', 29.99, 200, 'Wireless mouse'),
    ('Desk Chair', 'Furniture', 199.99, 30, 'Ergonomic office chair'),
    ('Coffee Mug', 'Kitchen', 12.99, 100, 'Ceramic coffee mug');

-- Insert sample data into orders
INSERT INTO orders (user_id, product_name, quantity, price, status) VALUES
    (1, 'Laptop', 1, 999.99, 'completed'),
    (1, 'Mouse', 2, 29.99, 'completed'),
    (2, 'Desk Chair', 1, 199.99, 'pending'),
    (3, 'Coffee Mug', 3, 12.99, 'shipped'),
    (4, 'Laptop', 1, 999.99, 'pending');

-- Create a second schema for testing multi-schema support
CREATE DATABASE IF NOT EXISTS analytics;

USE analytics;

-- Create a metrics table in the analytics schema
CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_id INT PRIMARY KEY AUTO_INCREMENT,
    metric_date DATE NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value DECIMAL(15, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample metrics data
INSERT INTO daily_metrics (metric_date, metric_name, metric_value) VALUES
    (CURDATE(), 'revenue', 5000.00),
    (CURDATE(), 'orders', 150.00),
    (CURDATE(), 'active_users', 1200.00),
    (DATE_SUB(CURDATE(), INTERVAL 1 DAY), 'revenue', 4800.00),
    (DATE_SUB(CURDATE(), INTERVAL 1 DAY), 'orders', 145.00);

-- Grant permissions to hoptimator user on both databases
GRANT ALL PRIVILEGES ON testdb.* TO 'hoptimator'@'%';
GRANT ALL PRIVILEGES ON analytics.* TO 'hoptimator'@'%';
FLUSH PRIVILEGES;
