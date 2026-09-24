package com.linkedin.hoptimator.pinot;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.Schema;

/**
 * Read surface for a Pinot cluster, used by {@link PinotSchema} and {@link PinotTable} to enumerate
 * tables and resolve their schemas.
 *
 * <p>This is the extension seam: the OSS {@link PinotControllerClient} talks to a Pinot controller
 * over REST, while downstream distributions can supply an implementation backed by a managed
 * control-plane (e.g. a gRPC gateway) without touching the Calcite wiring.
 */
public interface PinotClient extends AutoCloseable {

  /** Lists all table names known to the cluster. */
  List<String> listTables() throws IOException;

  /** Returns the Pinot schema for a table, or {@code null} if the table has no schema. */
  @Nullable
  Schema getSchema(String tableName) throws IOException;

  /** Returns {@code true} if a table with the given (raw) name exists. */
  boolean tableExists(String tableName) throws IOException;

  @Override
  default void close() throws IOException {
  }
}
