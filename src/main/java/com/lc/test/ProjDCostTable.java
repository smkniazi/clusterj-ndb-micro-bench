package com.lc.test;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

public class ProjDCostTable {

  public static final String TABLE_NAME = "yarn_projects_daily_cost";
  public static final String PROJECTNAME = "projectname";
  public static final String USER = "user";
  public static final String DAY = "day";
  public static final String CREDITS_USED = "credits_used";
  public static final String APP_IDS = "app_ids";
  @PersistenceCapable(table = TABLE_NAME)
  public interface ProjectDailyCostDTO {

    @PrimaryKey
    @Column(name = PROJECTNAME)
    String getProjectName();

    void setProjectName(String projectName);

    @PrimaryKey
    @Column(name = USER)
    String getUser();

    void setUser(String user);

    @PrimaryKey
    @Column(name = DAY)
    long getDay();

    void setDay(long day);

    @Column(name = CREDITS_USED)
    float getCreditUsed();

    void setCreditUsed(float credit);

    @Column(name = APP_IDS)
    String getAppIds();

    void setAppIds(String appIds);

  }
}
