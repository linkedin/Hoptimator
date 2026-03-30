package com.linkedin.hoptimator.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class RemoteRowListTest {

  @Mock
  private Api<String> mockApi;

  @Mock
  private RowMapper<String, Integer> mockMapper;

  private RemoteRowList<String, Integer> rowList;

  @BeforeEach
  void setUp() {
    rowList = new RemoteRowList<>(mockApi, mockMapper);
  }

  @Test
  void testSizeReturnsApiListSize() throws SQLException {
    when(mockApi.list()).thenReturn(Arrays.asList("a", "b", "c"));

    assertEquals(3, rowList.size());
  }

  @Test
  void testSizeThrowsRuntimeExceptionOnSqlException() throws SQLException {
    when(mockApi.list()).thenThrow(new SQLException("list failed"));

    RuntimeException ex = assertThrows(RuntimeException.class, () -> rowList.size());
    assertEquals("Could not list rows.", ex.getMessage());
  }

  @Test
  void testIteratorReturnsMappedRows() throws SQLException {
    when(mockApi.list()).thenReturn(Arrays.asList("a", "b"));
    when(mockMapper.toRow("a")).thenReturn(1);
    when(mockMapper.toRow("b")).thenReturn(2);

    Iterator<Integer> iter = rowList.iterator();

    assertTrue(iter.hasNext());
    assertEquals(1, iter.next());
    assertTrue(iter.hasNext());
    assertEquals(2, iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  void testIteratorThrowsRuntimeExceptionOnSqlException() throws SQLException {
    when(mockApi.list()).thenThrow(new SQLException("list failed"));

    RuntimeException ex = assertThrows(RuntimeException.class, () -> rowList.iterator());
    assertEquals("Could not list rows.", ex.getMessage());
  }

  @Test
  void testAddDelegatesToApiCreate() throws SQLException {
    when(mockMapper.fromRow(42)).thenReturn("answer");

    boolean result = rowList.add(42);

    assertTrue(result);
    verify(mockApi).create("answer");
  }

  @Test
  void testAddThrowsRuntimeExceptionOnSqlException() throws SQLException {
    when(mockMapper.fromRow(42)).thenReturn("answer");
    doThrow(new SQLException("create failed")).when(mockApi).create("answer");

    RuntimeException ex = assertThrows(RuntimeException.class, () -> rowList.add(42));
    assertEquals("Could not add row.", ex.getMessage());
  }

  @Test
  void testIteratorRemoveDelegatesToApiDelete() throws SQLException {
    when(mockApi.list()).thenReturn(Arrays.asList("a"));
    when(mockMapper.toRow("a")).thenReturn(1);

    Iterator<Integer> iter = rowList.iterator();
    iter.next();
    iter.remove();

    verify(mockApi).delete("a");
  }

  @Test
  void testIteratorRemoveThrowsRuntimeExceptionOnSqlException() throws SQLException {
    when(mockApi.list()).thenReturn(Arrays.asList("a"));
    when(mockMapper.toRow("a")).thenReturn(1);
    doThrow(new SQLException("delete failed")).when(mockApi).delete("a");

    Iterator<Integer> iter = rowList.iterator();
    iter.next();

    RuntimeException ex = assertThrows(RuntimeException.class, iter::remove);
    assertEquals("Could not remove row.", ex.getMessage());
  }
}
