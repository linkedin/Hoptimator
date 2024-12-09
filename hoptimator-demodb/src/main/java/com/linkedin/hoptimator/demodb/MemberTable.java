package com.linkedin.hoptimator.demodb;

import com.linkedin.hoptimator.util.ArrayTable;


public class MemberTable extends ArrayTable<MemberTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public String FIRST_NAME;
    public String LAST_NAME;
    public String MEMBER_URN;
    public String COMPANY_URN;

    public Row(String firstName, String lastName, String memberUrn, String companyUrn) {
      this.FIRST_NAME = firstName;
      this.LAST_NAME = lastName;
      this.MEMBER_URN = memberUrn;
      this.COMPANY_URN = companyUrn;
    }
  }
  // CHECKSTYLE:ON

  public MemberTable() {
    super(Row.class);
    rows().add(new Row("Alice", "Addison", "urn:li:member:123", "urn:li:company:linkedin"));
    rows().add(new Row("Bob", "Baker", "urn:li:member:456", "urn:li:company:linkedin"));
    rows().add(new Row("Charlie", "Chapman", "urn:li:member:789", "urn:li:company:microsoft"));
  }
}
