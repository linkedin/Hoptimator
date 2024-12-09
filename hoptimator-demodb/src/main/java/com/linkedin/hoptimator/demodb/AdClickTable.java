package com.linkedin.hoptimator.demodb;

import com.linkedin.hoptimator.util.ArrayTable;


public class AdClickTable extends ArrayTable<AdClickTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public String CAMPAIGN_URN;
    public String MEMBER_URN;

    public Row(String campaignUrn, String memberUrn) {
      this.CAMPAIGN_URN = campaignUrn;
      this.MEMBER_URN = memberUrn;
    }
  }
  // CHECKSTYLE:ON

  public AdClickTable() {
    super(Row.class);
    rows().add(new Row("urn:li:campaign:100", "urn:li:member:456"));
    rows().add(new Row("urn:li:campaign:101", "urn:li:member:789"));
  }
}
