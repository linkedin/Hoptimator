package com.linkedin.hoptimator.demodb;

import com.linkedin.hoptimator.util.ArrayTable;


public class CampaignTable extends ArrayTable<CampaignTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public String CAMPAIGN_URN;
    public String COMPANY_URN;

    public Row(String campaignUrn, String companyUrn) {
      this.CAMPAIGN_URN = campaignUrn;
      this.COMPANY_URN = companyUrn;
    }
  }
  // CHECKSTYLE:ON

  public CampaignTable() {
    super(Row.class);
    rows().add(new Row("urn:li:campaign:100", "urn:li:company:microsoft"));
    rows().add(new Row("urn:li:campaign:101", "urn:li:company:microsoft"));
  }
}
