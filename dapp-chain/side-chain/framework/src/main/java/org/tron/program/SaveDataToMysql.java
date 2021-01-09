package org.tron.program;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class SaveDataToMysql extends Thread {

  ConcurrentLinkedQueue<SyncDataToDB.BaseBalance> queue = new ConcurrentLinkedQueue<>();
  PreparedStatement preparedStatement;
  private AtomicLong count = new AtomicLong(0);
  volatile boolean over = false;
  volatile boolean runOver = false;
  int assetType;
  String tableName;
  AtomicLong maxId;
  Statement statement;
  String sql;

  public void setOver() {
    this.over = true;
  }

  public boolean isOver() {
    return runOver && queue.isEmpty();
  }

  public SaveDataToMysql(
//                         PreparedStatement preparedStatement,
                          Statement statement,
                          int assetType,
                         String tableName,
                         AtomicLong maxId,
                         String sql
                         ) {
//    this.preparedStatement = preparedStatement;
    this.statement = statement;
    this.assetType = assetType;
    this.tableName = tableName;
    this.maxId = maxId;
    this.sql = sql;
  }

  List<SyncDataToDB.TrxBalanceInfo> trxBalanceInfoList = new ArrayList<>();
  List<SyncDataToDB.Trc10BalanceInfo> trc10BalanceInfoList = new ArrayList<>();
  List<SyncDataToDB.BalanceInfo> trc20BalanceInfoList = new ArrayList<>();

  @Override
  public void run() {
    while (!over || !queue.isEmpty()) {
      try {
        SyncDataToDB.BaseBalance info = queue.poll();

        if (info == null) {
          continue;
        }

        info.setId(maxId.getAndIncrement());
        addList(info);
        final long l = count.incrementAndGet();

        if (l % 300 == 0) {
          executeBatch();
          logger.info(" >>>>> tableName:{}, count:{}", tableName, l);
        }
      } catch (Throwable e) {
        logger.error("", e);
      }
    }

    try {
      executeBatch();
      logger.info(" >>>>> tableName:{}, over.  count{}", tableName, count.get());
      runOver = true;
    } catch (Throwable e) {
      logger.error("", e);
    }
  }


  public static final int TRX = 0;
  public static final int TRC10 = 1;
  public static final int TRC20 = 2;

  public void insert(PreparedStatement preparedStatement, SyncDataToDB.BaseBalance item, int assetType) {
    try {
      int index = 1;
      preparedStatement.setLong(index++, maxId.incrementAndGet());
      preparedStatement.setString(index++, item.accountAddress);
      preparedStatement.setString(index++, item.balance.toString());
      preparedStatement.setLong(index++, item.blockNum);
      preparedStatement.setString(index++, item.balance.toString());
      preparedStatement.setLong(index++, item.blockNum);

      if (assetType == TRX) {
//        System.out.println(" >>> index qian:" + index);
        setBalance(item, index, preparedStatement);
        // index 是基本类型，这里自增需要额外处理
        index = index + 7;
//        System.out.println(" >>> index hou:" + index);
      }
      else if (assetType == TRC10) {
        SyncDataToDB.Trc10BalanceInfo info = (SyncDataToDB.Trc10BalanceInfo) item;
        preparedStatement.setString(index++, info.getTokenId());
      }
      else if (assetType == TRC20) {
        SyncDataToDB.BalanceInfo info = (SyncDataToDB.BalanceInfo) item;
        preparedStatement.setString(index++, info.tokenAddress);
        preparedStatement.setString(index++, info.decimals.toString());
      }

      preparedStatement.setLong(index++, 1);
      Timestamp now = Timestamp.valueOf(LocalDateTime.now());
      preparedStatement.setTimestamp(index++, now);
      preparedStatement.setTimestamp(index++, now);
      preparedStatement.addBatch();
      preparedStatement.clearParameters();
    } catch (Exception e) {
      logger.error("assetType:" + assetType, e);
    }
  }


  public void addList(SyncDataToDB.BaseBalance item) {
      if (TRX == assetType) {
        trxBalanceInfoList.add((SyncDataToDB.TrxBalanceInfo) item);
      } else if (TRC10 == assetType) {
        trc10BalanceInfoList.add((SyncDataToDB.Trc10BalanceInfo) item);
      } else if (TRC20 == assetType) {
        trc20BalanceInfoList.add((SyncDataToDB.BalanceInfo) item);
      }
  }

  public void executeBatch() {
    if (TRX == assetType) {
      executeTrxBatch();
    } else if (TRC10 == assetType) {
      executeTrc10Batch();
    } else if (TRC20 == assetType) {
      executeTrc20Batch();
    }
  }

  private String getValueOrNull(Object fieldValue) {
    if (null == fieldValue) {
      return "null";
    }

    if (StringUtils.isEmpty(fieldValue.toString())) {
      return "null";
    }

    return "'" + fieldValue.toString() + "'";
  }

  private Object getStrDate(Date date) {
    if (null == date) {
      return null;
    }
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    return format.format(date);
  }


  public void executeTrxBatch() {
    String executeSql = "";
    try {
      if (CollectionUtils.isEmpty(trxBalanceInfoList)) {
        return;
      }

      AtomicInteger index = new AtomicInteger();
      StringBuilder sb = new StringBuilder();

      trxBalanceInfoList.forEach(item -> {
        sb.append("(");
        sb.append(item.getId()).append(",");
        sb.append(getValueOrNull(item.accountAddress)).append(",");
        sb.append(getValueOrNull(item.balance.toString())).append(",");
        sb.append(getValueOrNull(item.blockNum)).append(",");
        sb.append(getValueOrNull(item.balance.toString())).append(",");
        sb.append(getValueOrNull(item.blockNum)).append(",");
        sb.append(getValueOrNull(item.getFrozenBalance().toString())).append(",");
        sb.append(getValueOrNull(item.getEnergyFrozenBalance())).append(",");
        sb.append(getValueOrNull(item.getDelegatedFrozenBalanceForEnergy())).append(",");
        sb.append(getValueOrNull(item.getDelegatedFrozenBalanceForBandwidth())).append(",");
        sb.append(getValueOrNull(item.getFrozenSupplyBalance())).append(",");
        sb.append(getValueOrNull(item.getAcquiredDelegatedFrozenBalanceForEnergy())).append(",");
        sb.append(getValueOrNull(item.getAcquiredDelegatedFrozenBalanceForBandwidth())).append(",");
        sb.append(1).append(",");

        String date = getValueOrNull(getStrDate(new Date()));
        sb.append(date).append(",");
        sb.append(date);
        sb.append(")");

        if (index.getAndIncrement() < trxBalanceInfoList.size() - 1) {
          sb.append(",");
        }
      });

      executeSql = String.format(sql, tableName, sb.toString());
      statement.execute(executeSql);
      trxBalanceInfoList.clear();
    } catch (SQLException e) {
      logger.error("executeTrxBatch error, executeSql={}", executeSql, e);
    }
  }


  public void executeTrc10Batch() {
    String executeSql = "";
    try {
      if (CollectionUtils.isEmpty(trc10BalanceInfoList)) {
        return;
      }

      AtomicInteger index = new AtomicInteger();
      StringBuilder sb = new StringBuilder();

      trc10BalanceInfoList.forEach(item -> {
        sb.append("(");
        sb.append(item.getId()).append(",");
        sb.append(getValueOrNull(item.getAccountAddress())).append(",");
        sb.append(getValueOrNull(item.getBalance())).append(",");
        sb.append(getValueOrNull(item.getBlockNum())).append(",");
        sb.append(getValueOrNull(item.getBalance())).append(",");
        sb.append(getValueOrNull(item.getBlockNum())).append(",");
        sb.append(getValueOrNull(item.getTokenId())).append(",");
        sb.append(1).append(",");
        String date = getValueOrNull(getStrDate(new Date()));
        sb.append(date).append(",");
        sb.append(date);
        sb.append(")");

        if (index.getAndIncrement() < trc10BalanceInfoList.size() - 1) {
          sb.append(",");
        }
      });

      executeSql = String.format(sql, tableName, sb.toString());
      statement.execute(executeSql);
      trc10BalanceInfoList.clear();
    } catch (SQLException e) {
      logger.error("executeTrc10Batch error, executeSql={}", executeSql, e);
    }
  }


  public void executeTrc20Batch() {
    String executeSql = "";
    try {
      if (CollectionUtils.isEmpty(trc20BalanceInfoList)) {
        return;
      }

      AtomicInteger index = new AtomicInteger();
      StringBuilder sb = new StringBuilder();

      trc20BalanceInfoList.forEach(item -> {
        sb.append("(");
        sb.append(item.getId()).append(",");
        sb.append(getValueOrNull(item.getAccountAddress())).append(",");
        sb.append(getValueOrNull(item.getBalance())).append(",");
        sb.append(getValueOrNull(item.getBlockNum())).append(",");
        sb.append(getValueOrNull(item.getBalance())).append(",");
        sb.append(getValueOrNull(item.getBlockNum())).append(",");
        sb.append(getValueOrNull(item.getTokenAddress())).append(",");
        sb.append(getValueOrNull(item.getDecimals())).append(",");
        sb.append(1).append(",");
        String date = getValueOrNull(getStrDate(new Date()));
        sb.append(date).append(",");
        sb.append(date);
        sb.append(")");

        if (index.getAndIncrement() < trc20BalanceInfoList.size() - 1) {
          sb.append(",");
        }
      });

      executeSql = String.format(sql, tableName, sb.toString());
      statement.execute(executeSql);
      trc20BalanceInfoList.clear();
    } catch (SQLException e) {
      logger.error("executeTrc20Batch error, executeSql={}", executeSql, e);
    }
  }




  private static void setBalance(SyncDataToDB.BaseBalance item, int index, PreparedStatement preparedStatement) throws SQLException {
    SyncDataToDB.TrxBalanceInfo info = (SyncDataToDB.TrxBalanceInfo) item;
    preparedStatement.setString(index++, info.getFrozenBalance().toString());
    preparedStatement.setString(index++, info.getEnergyFrozenBalance().toString());
    preparedStatement.setString(index++, info.getDelegatedFrozenBalanceForEnergy().toString());
    preparedStatement.setString(index++, info.getDelegatedFrozenBalanceForBandwidth().toString());
    preparedStatement.setString(index++, info.getFrozenSupplyBalance().toString());
    preparedStatement.setString(index++, info.getAcquiredDelegatedFrozenBalanceForEnergy().toString());
    preparedStatement.setString(index++, info.getAcquiredDelegatedFrozenBalanceForBandwidth().toString());
  }

}
