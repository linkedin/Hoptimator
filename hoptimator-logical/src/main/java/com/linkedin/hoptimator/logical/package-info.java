/**
 * Logical Table — a single abstraction over multiple physical storage tiers
 * (e.g., Kafka for nearline, OpenHouse/Iceberg for offline, Venice for online).
 *
 * <p>The driver URL format is:
 * <pre>jdbc:logical://nearline=xinfra-tracking;offline=openhouse;online=venice</pre>
 *
 * <p>Each {@code {tier-name}={database-crd-name}} parameter declares a tier and
 * the physical resources for that tier come from the named Database CRD.
 */
package com.linkedin.hoptimator.logical;
