package com.linkedin.hoptimator.pinot;

import java.util.List;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.LikePattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PinotSchemaTest {

  @Test
  void loadAllListsTables() throws Exception {
    PinotClient client = mock(PinotClient.class);
    when(client.listTables()).thenReturn(List.of("t1", "t2"));

    PinotSchema schema = new PinotSchema(client);

    assertThat(schema.tables().getNames(LikePattern.any())).contains("t1", "t2");
  }

  @Test
  void loadResolvesExistingTable() throws Exception {
    PinotClient client = mock(PinotClient.class);
    when(client.tableExists("t1")).thenReturn(true);

    Table table = new PinotSchema(client).tables().get("t1");

    assertThat(table).isInstanceOf(PinotTable.class);
  }

  @Test
  void loadReturnsNullForMissingTable() throws Exception {
    PinotClient client = mock(PinotClient.class);
    when(client.tableExists("nope")).thenReturn(false);

    assertThat(new PinotSchema(client).tables().get("nope")).isNull();
  }

  @Test
  void tablesReturnsSameLookupInstance() {
    PinotClient client = mock(PinotClient.class);
    PinotSchema schema = new PinotSchema(client);

    assertThat(schema.tables()).isSameAs(schema.tables());
  }
}
