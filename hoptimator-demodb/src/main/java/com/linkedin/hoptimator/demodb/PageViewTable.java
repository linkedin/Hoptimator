package com.linkedin.hoptimator.demodb;

import com.linkedin.hoptimator.util.ArrayTable;


public class PageViewTable extends ArrayTable<PageViewTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public String PAGE_URN;
    public String MEMBER_URN;

    public Row(String pageUrn, String memberUrn) {
      this.PAGE_URN = pageUrn;
      this.MEMBER_URN = memberUrn;
    }
  }
  // CHECKSTYLE:ON

  public PageViewTable() {
    super(Row.class);
    rows().add(new Row("urn:li:page:10000", "urn:li:member:123"));
    rows().add(new Row("urn:li:page:10001", "urn:li:member:456"));
  }
}
