package com.yongpoliu.sequence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

/**
 * Sequence工厂类
 */
public class SequenceFactory {

  private DataSource dataSource;

  private String tableName;

  private int cacheSize;

  private String seqName;

  public Sequence createSeq() {
    final Map<String, Long> map = refreshSeq();

    CacheableSequence cacheableSequence = new CacheableSequence(seqName);
    cacheableSequence.setCurrentValue(map.get("init"));
    cacheableSequence.setMaxValue(map.get("maxValue"));
    return cacheableSequence;
  }


  Map<String, Long> refreshSeq() {
    Map<String, Long> map = new HashMap<String, Long>();
    try {
      Connection connection = dataSource.getConnection();
      connection.setAutoCommit(false);
      while (true) {
        PreparedStatement
            preparedStatement =
            connection
                .prepareStatement("update " + tableName + " set current_value = current_value + "
                                  + cacheSize + " where seqName = " + seqName);
        if (preparedStatement.executeUpdate() > 0) {
          final ResultSet
              resultSet =
              preparedStatement.executeQuery(
                  "select current_value from " + tableName + " where seq_name = " + seqName);
          long max = resultSet.getLong("current_value");
          map.put("init", max - cacheSize);
          map.put("maxValue", max);
          break;
        } else {
          long currentValue = cacheSize + 1;
          int i = preparedStatement.executeUpdate("insert ignore " + tableName
                                                  + " valus (null, " + seqName
                                                  + ", " + currentValue + ", now(), now()" + ")");
          if (i > 0) {
            map.put("init", 1L);
            map.put("maxValue",currentValue);
            break;
          }
        }
      }
      connection.commit();
      connection.close();
    } catch (SQLException e) {
      throw new SequenceException(seqName, e);
    }
    return map;
  }
}
