package com.linkedin.hoptimator.demodb;

import com.linkedin.hoptimator.util.ArrayTable;


public class CompanyTable extends ArrayTable<CompanyTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public final String COMPANY_NAME;
    public final String COMPANY_URN;

    public Row(String companyName, String companyUrn) {
      this.COMPANY_NAME = companyName;
      this.COMPANY_URN = companyUrn;
    }
  }
  // CHECKSTYLE:ON

  public CompanyTable() {
    super(Row.class);
    rows().add(new Row("LinkedIn", "urn:li:company:linkedin"));
    rows().add(new Row("Microsoft", "urn:li:company:microsoft"));
  }
}
