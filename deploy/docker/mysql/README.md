# MySQL Docker Setup for Hoptimator

This directory contains the Docker Compose configuration and initialization scripts for running MySQL with Hoptimator.

## Overview

The setup creates a MySQL 8.0 instance with:
- **Two databases**: `testdb` and `analytics`
- **Pre-populated test data** for integration testing
- **Catalog support** demonstrating Hoptimator's hierarchical data source capabilities

## Quick Start

```bash
# Deploy MySQL with Hoptimator
make deploy-mysql

# Undeploy MySQL
make undeploy-mysql
```

## Database Schema

### testdb Database

**users table:**
- `user_id` (INT, PRIMARY KEY)
- `username` (VARCHAR)
- `email` (VARCHAR)
- `created_at` (TIMESTAMP)
- `is_active` (BOOLEAN)

**orders table:**
- `order_id` (INT, PRIMARY KEY)
- `user_id` (INT, FOREIGN KEY)
- `product_name` (VARCHAR)
- `quantity` (INT)
- `price` (DECIMAL)
- `order_date` (TIMESTAMP)
- `status` (VARCHAR)

**products table:**
- `product_id` (INT, PRIMARY KEY)
- `product_name` (VARCHAR)
- `category` (VARCHAR)
- `price` (DECIMAL)
- `stock_quantity` (INT)
- `description` (TEXT)

### analytics Database

**daily_metrics table:**
- `metric_id` (INT, PRIMARY KEY)
- `metric_date` (DATE)
- `metric_name` (VARCHAR)
- `metric_value` (DECIMAL)
- `created_at` (TIMESTAMP)

## Connection Details

- **Host**: `localhost` (or `host.docker.internal` from containers)
- **Port**: `3306`
- **Root Password**: `rootpassword`
- **User**: `hoptimator`
- **Password**: `hoptimator123`
- **Databases**: `testdb`, `analytics`

## Sample Queries

Once deployed, you can query MySQL through Hoptimator:

```sql
-- Query users from testdb
SELECT * FROM "MYSQL"."testdb"."users";
```

## Kubernetes Resources

The deployment creates:
- **mysql-testdb** Database resource (catalog: testdb)
- **mysql-analytics** Database resource (catalog: analytics)
- **mysql-read-template** TableTemplate for reading
- **mysql-write-template** TableTemplate for writing

## Testing Catalog Support

This setup demonstrates Hoptimator's catalog support:

1. **3-level naming**: `[catalog, schema, table]` internally
2. **Multiple schemas**: Both `testdb` and `analytics` are accessible
3. **JDBC metadata**: Schemas & tables are discovered automatically via MySQL JDBC metadata
4. **Type mapping**: MySQL types are correctly mapped to Calcite types

## Direct MySQL Access

You can also connect directly to MySQL for debugging:

```bash
# Connect to MySQL container
docker exec -it hoptimator-mysql mysql -u hoptimator -phoptimatorpassword

mysql> show databases;
+--------------------+
| Database           |
+--------------------+
| analytics          |
| information_schema |
| performance_schema |
| testdb             |
+--------------------+
4 rows in set (0.01 sec)
```

## Customization

To add more tables or data:
1. Edit `init.sql` with your schema and data
2. Run `make undeploy-mysql` to remove the old container
3. Run `make deploy-mysql` to recreate with new schema

## Troubleshooting

**Container won't start:**
```bash
# Check logs
docker logs hoptimator-mysql

# Ensure port 3306 is not in use
lsof -i :3306
```

**Connection refused:**
```bash
# Wait for MySQL to be ready (healthcheck)
docker compose -f ./deploy/docker/mysql/docker-compose.yaml ps

# Test connection
docker exec hoptimator-mysql mysqladmin ping -h localhost -u hoptimator -phoptimatorpassword
```

**Tables not created:**
```bash
# Check if init.sql ran
docker exec -it hoptimator-mysql mysql -u hoptimator -phoptimatorpassword -e "SHOW DATABASES;"
docker exec -it hoptimator-mysql mysql -u hoptimator -phoptimatorpassword testdb -e "SHOW TABLES;"
```
