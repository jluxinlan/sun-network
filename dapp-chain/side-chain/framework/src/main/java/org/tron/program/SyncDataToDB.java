package org.tron.program;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.tron.core.capsule.BlockCapsule;
import redis.clients.jedis.Jedis;
import redis.clients.util.Hashing;
import redis.clients.util.MurmurHash;

import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class SyncDataToDB extends Thread {

  // todo 配置文件
  public static String mysqlTrxUrl = "jdbc:mysql://127.0.0.1:3306/tron_nile";
  public static String mysqlTrxUsername = "root";
  public static String mysqlTrxPass = "tron";

  public static String mysqlTrc10Url = "jdbc:mysql://127.0.0.1:3306/tron_nile";
  public static String mysqlTrc10Username = "root";
  public static String mysqlTrc10Pass = "tron";


  public static String mysqlTrc20Url = "jdbc:mysql://127.0.0.1:3306/tron_nile";
  public static String mysqlTrc20Username = "root";
  public static String mysqlTrc20Pass = "tron";

  private synchronized static Connection getConnection(String url, String name, String pass) {
    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
      // Setup the connection with the DB

      String tmpUrl;

      if (url.contains("?")) {
        tmpUrl = url + "&serverTimezone=Hongkong";
      }
      else {
        tmpUrl = url + "?serverTimezone=Hongkong";
      }

      Connection connect = DriverManager.getConnection(tmpUrl, name, pass);
      connect.setAutoCommit(true);
      return connect;
    } catch (Exception e) {
      System.out.println(" >>> create conn error");
      logger.error(e.getMessage(), e);
    }

    return null;
  }


  private synchronized static Connection getConnection(int type) {
    try {
      if (type == TRX) {
        return getConnection(mysqlTrxUrl, mysqlTrxUsername, mysqlTrxPass);
      }
      else if (type == TRC10) {
        return getConnection(mysqlTrc10Url, mysqlTrc10Username, mysqlTrc10Pass);
      }
      else if (type == TRC20) {
        return getConnection(mysqlTrc20Url, mysqlTrc20Username, mysqlTrc20Pass);
      }
    } catch (Exception e) {
      System.out.println(" >>> create conn error");
      logger.error(e.getMessage(), e);
    }

    return null;
  }

  @Override
  public void run() {
    saveAll();
  }

  @Data
  public static class BaseBalance {
    Long id;
    String accountAddress;
    Long blockNum;
    BigInteger balance;
  }

  @Data
  public static class BalanceInfo extends BaseBalance {
    String tokenAddress;
    Integer decimals;
  }

  @Data
  public static class Trc10BalanceInfo extends BaseBalance {
    String tokenId;
  }

  @Data
  public static class TrxBalanceInfo extends BaseBalance {
    BigInteger frozenBalance;
    BigInteger energyFrozenBalance;
    BigInteger delegatedFrozenBalanceForEnergy;
    BigInteger delegatedFrozenBalanceForBandwidth;
    BigInteger frozenSupplyBalance;
    BigInteger acquiredDelegatedFrozenBalanceForEnergy;
    BigInteger acquiredDelegatedFrozenBalanceForBandwidth;
  }

  public static final int TRX = 0;
  public static final int TRC10 = 1;
  public static final int TRC20 = 2;

  private static final String SAVE_ALL = "saveAll";
  private static final String INSERT = "insert";
  private static final String UPDATE = "update";

  private static final String querySql = "select id from %s where account_address = ? and token_address = ?";
  private static final String querySql10 = "select id from %s where account_address = ? and token_id = ?";
  private static final String querySqlTrx = "select id from %s where account_address = ? ";

  private volatile boolean isOver = false;
  private volatile boolean runOver = false;

  public void setOver() {
    isOver = true;
  }

  public boolean isOver() {
    return runOver && queue.isEmpty();
  }

  public SyncDataToDB(ConcurrentLinkedQueue<BaseBalance> queue, int assetType) {
    this.queue = queue;
    this.assetType = assetType;
  }

  private ConcurrentLinkedQueue<BaseBalance> queue;
  private int assetType;

  private Map<String, SaveDataToMysql> insertMap = new HashMap();
  private Map<String, PreparedStatement> updateMap = new HashMap();
  private Map<String, AtomicLong> insertCountMap = new HashMap();
  private Map<String, AtomicLong> updateCountMap = new HashMap();

  private AtomicLong id = new AtomicLong(1);


  public void saveAll() {
//    Connection connection = getConnection();
    try {
      AtomicLong count = new AtomicLong(0);
      logger.info(" >>>> {} queue run start!", assetType);

      while (!isOver || !queue.isEmpty()) {
        try {
          BaseBalance info = queue.poll();

          if (info == null) {
            continue;
          }

//          PreparedStatement statement = getStatement(connection, SAVE_ALL, assetType, info.getAccountAddress());
//          statement.setString(1, info.getAccountAddress());
//
//          if (assetType == TRX) {
//            // do nothing
//          }
//          else if (assetType == TRC10) {
//            Trc10BalanceInfo balanceInfo = (Trc10BalanceInfo) info;
//            statement.setString(2, balanceInfo.getTokenId());
//          }
//          else {
//            BalanceInfo balanceInfo = (BalanceInfo) info;
//            statement.setString(2, balanceInfo.getTokenAddress());
//          }
//
//          final ResultSet resultSet = statement.executeQuery();
          final String tableName = getTableName(assetType, info.getAccountAddress());
//          if (resultSet.next()) {
//            // 有数据就 update
//            final long id = resultSet.getLong(1);
//            info.setId(id);
//
//            PreparedStatement preparedStatement = updateMap.get(tableName);
//            if (preparedStatement == null) {
//              preparedStatement = getStatement(connection, UPDATE, assetType, info.getAccountAddress());
//              updateMap.put(tableName, preparedStatement);
//              updateCountMap.put(tableName, new AtomicLong(0));
//            }
//
//            update(preparedStatement, info, assetType);
//
//            if (updateCountMap.get(tableName).incrementAndGet() % 1000 == 0) {
//              logger.info(" >>>>> update batch:{}", info);
//              preparedStatement.executeBatch();
//            }
//          }
//          else {
          SaveDataToMysql toMysql = insertMap.get(tableName);
          if (toMysql == null) {
            Connection connection = getConnection(assetType);
//            PreparedStatement preparedStatement = getStatement(connection, INSERT, assetType, tableName);
            toMysql = new SaveDataToMysql(connection.createStatement(), assetType, tableName, id, getSql(INSERT, assetType));
            insertMap.put(tableName, toMysql);
            toMysql.start();
            clearTable(connection, tableName);
          }

          toMysql.queue.add(info);

//          }

          if (count.incrementAndGet() % 20000 == 0) {
            logger.info(" >>>> info:{}", info);
          }
        } catch (Exception e) {
          logger.error("",e);
        }
      }

      insertMap.forEach((key, statement) -> {
        statement.setOver();
        logger.info(" >>>>> assetType:{}, table:{}, insert over ", assetType, key);
      });

      while (!allOver()) {
        try {
          Thread.sleep(500);
        } catch (Exception ex) {

        }
      }

      runOver = true;


//      updateMap.forEach((key, statement) -> {
//        try {
//          statement.executeBatch();
//          logger.info(" >>>>> update over batch:{}", key);
//        } catch (SQLException e) {
//          logger.error("", e);
//        }
//      });

      logger.info(" >>>> {} queue run over!", assetType);
//      insert(connection, insertInfos, assetType);
//      update(connection, updateInfos, assetType);
    } catch (Exception e) {
      logger.error("", e);
    }
  }


  private void clearTable(Connection connection, String tableName) {
    String sql = "truncate " + tableName;
    try {
      PreparedStatement statement = connection.prepareStatement(sql);
      statement.executeUpdate();
      statement.close();
    } catch (Exception ex) {
      logger.error("", ex);
    }
  }

  private boolean allOver() {
    return insertMap.values().stream().allMatch(item -> item.isOver());
  }

  private static PreparedStatement getStatement(Connection connection,
                                                String sqlType, int assetType,
                                                String tableName) throws SQLException {
    String sql = getSql(sqlType, assetType);
    sql = String.format(sql, tableName);
    return connection.prepareStatement(sql);
  }


  private static String getSql(String sqlType, int assetType) {
    if (SAVE_ALL.equals(sqlType)) {
      switch (assetType) {
        case TRX:
          return querySqlTrx;
        case TRC10:
          return querySql10;
        case TRC20:
          return querySql;
      }
    }
    else if (INSERT.equals(sqlType)) {
      switch (assetType) {
        case TRX:
          return insertSqlTrxBatch;
        case TRC10:
          return insertSqlTrc10Batch;
        case TRC20:
          return insertSqlTrc20Batch;
      }
    }
    else if (UPDATE.equals(sqlType)) {
      switch (assetType) {
        case TRX:
          return updatSqlTrx;
        case TRC10:
          return updatSql10;
        case TRC20:
          return updatSql;
      }
    }

    return null;
  }


  private static final String trxTable = "balance_info_trx";
  private static final String trc10Table = "balance_info_trc10";
  private static final String trc20Table = "balance_info_trc20";

  private static final Hashing MURMUR_HASH = new MurmurHash();

  private static String getTableName(int assetType, String accountAddress) {
    if (StringUtils.isEmpty(accountAddress)) {
      throw  new RuntimeException(" accountAddress is empty");
    }

    long index = Math.abs(MURMUR_HASH.hash(accountAddress) % 20);;
    switch (assetType) {
      case TRX:
        return trxTable + "_" + index;
      case TRC10:
        return trc10Table + "_" + index;
      case TRC20:
        return trc20Table + "_" + index;
    }

    logger.error(" >>>>> assetType:{}, accountAddress:{}", assetType, accountAddress);
    return null;
  }

  private static final String insertSql = "insert into %s (id, account_address, balance, block_num, solidity_balance, " +
      "solidity_block_num, token_address, decimals, version, created_time, updated_time) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  private static final String insertSql10 = "insert into %s (id, account_address, balance, block_num, solidity_balance, solidity_block_num, token_id, version, created_time, updated_time) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  private static final String insertSqlTrx = "insert into %s (id, account_address, balance, block_num, solidity_balance, " +
      "solidity_block_num, " +
          " frozen_balance, energy_frozen_balance, delegated_frozen_balance_for_energy, " +
          " delegated_frozen_balance_for_bandwidth, frozen_supply_balance, acquired_delegated_frozen_balance_for_energy, " +
          " acquired_delegated_frozen_balance_for_bandwidth, " +
          " version, created_time, updated_time) values (?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?)";




  public static final String insertSqlTrc20Batch = "insert into %s (id, account_address, balance, block_num, " +
      "solidity_balance, solidity_block_num, token_address, decimals, version, created_time, updated_time)  values %s";
  public static final String insertSqlTrc10Batch = "insert into %s (id, account_address, balance, block_num, " +
      "solidity_balance, solidity_block_num, token_id, version, created_time, updated_time)  values %s";
  public static final String insertSqlTrxBatch = "insert into %s (id, account_address, balance, block_num, solidity_balance, solidity_block_num, " +
      " frozen_balance, energy_frozen_balance, delegated_frozen_balance_for_energy, " +
      " delegated_frozen_balance_for_bandwidth, frozen_supply_balance, acquired_delegated_frozen_balance_for_energy, " +
      " acquired_delegated_frozen_balance_for_bandwidth, " +
      " version, created_time, updated_time) values %s";






//  private void insert(Connection connection, List<BaseBalance> infos, int assetType) {
//    if (CollectionUtils.isEmpty(infos)) {
//      return;
//    }
//
//    try {
//      infos.forEach(item -> {
//        insert(connection, item, assetType);
//      });
//    }
//    catch (Exception ex) {
//      logger.error("", ex);
//    }
//  }
//
//  private void insert(Connection connection, BaseBalance item, int assetType) {
//    if (item == null) {
//      return;
//    }
//
//    try {
//      PreparedStatement preparedStatement = getStatement(connection, INSERT, assetType, item.getAccountAddress());
//      int index = 1;
//      preparedStatement.setLong(index++, getNextID());
//      preparedStatement.setString(index++, item.accountAddress);
//      preparedStatement.setString(index++, item.balance.toString());
//      preparedStatement.setLong(index++, item.blockNum);
//      preparedStatement.setString(index++, item.balance.toString());
//      preparedStatement.setLong(index++, item.blockNum);
//
//      if (assetType == TRX) {
////        System.out.println(" >>> index qian:" + index);
//        setBalance(item, index, preparedStatement);
//        // index 是基本类型，这里自增需要额外处理
//        index = index + 7;
////        System.out.println(" >>> index hou:" + index);
//      }
//      else if (assetType == TRC10) {
//        Trc10BalanceInfo info = (Trc10BalanceInfo) item;
//        preparedStatement.setString(index++, info.getTokenId());
//      }
//      else if (assetType == TRC20) {
//        BalanceInfo info = (BalanceInfo) item;
//        preparedStatement.setString(index++, info.tokenAddress);
//        preparedStatement.setString(index++, info.decimals.toString());
//      }
//
//      preparedStatement.setLong(index++, 1);
//      Timestamp now = Timestamp.valueOf(LocalDateTime.now());
//      preparedStatement.setTimestamp(index++, now);
//      preparedStatement.setTimestamp(index++, now);
//
//      preparedStatement.execute();
//      preparedStatement.close();
//    } catch (Exception e) {
//      logger.error("assetType:" + assetType, e);
//    }
////        preparedStatement.exe();
//
//  }

//
//  public void insert(PreparedStatement preparedStatement, BaseBalance item, int assetType) {
//    if (item == null) {
//      return;
//    }
//
//    try {
//      int index = 1;
//      preparedStatement.setLong(index++, getNextID());
//      preparedStatement.setString(index++, item.accountAddress);
//      preparedStatement.setString(index++, item.balance.toString());
//      preparedStatement.setLong(index++, item.blockNum);
//      preparedStatement.setString(index++, item.balance.toString());
//      preparedStatement.setLong(index++, item.blockNum);
//
//      if (assetType == TRX) {
////        System.out.println(" >>> index qian:" + index);
//        setBalance(item, index, preparedStatement);
//        // index 是基本类型，这里自增需要额外处理
//        index = index + 7;
////        System.out.println(" >>> index hou:" + index);
//      }
//      else if (assetType == TRC10) {
//        Trc10BalanceInfo info = (Trc10BalanceInfo) item;
//        preparedStatement.setString(index++, info.getTokenId());
//      }
//      else if (assetType == TRC20) {
//        BalanceInfo info = (BalanceInfo) item;
//        preparedStatement.setString(index++, info.tokenAddress);
//        preparedStatement.setString(index++, info.decimals.toString());
//      }
//
//      preparedStatement.setLong(index++, 1);
//      Timestamp now = Timestamp.valueOf(LocalDateTime.now());
//      preparedStatement.setTimestamp(index++, now);
//      preparedStatement.setTimestamp(index++, now);
//      preparedStatement.addBatch();
//    } catch (Exception e) {
//      logger.error("assetType:" + assetType, e);
//    }
//  }

  public static final String MAX_ID_KEY = "tron-link-data-init-max-id";
  private synchronized Long getMaxIdByRedis() {
    String s = null;
    try {
      final Jedis conn = tryGetConn();

      if (conn == null) {
        return null;
      }

      s = conn.get(MAX_ID_KEY);

      if (StringUtils.isEmpty(s)) {
        return null;
      }

      final long l = Long.parseLong(s);
      return l;
    } catch (Exception ex) {
      logger.info(" >>>> maxid get error:{}", s);
      // ignore it.
    }

    return null;
  }

  private void setMaxIdKey(long maxId) {
    final Jedis conn = tryGetConn();

    if (conn == null) {
      return;
    }

    try {
      conn.set(MAX_ID_KEY, String.valueOf(maxId));
    }
    catch (Exception ex) {
      logger.info(" >>> set maxID error:", ex);
    }
  }

  private static void setBalance(BaseBalance item, int index, PreparedStatement preparedStatement) throws SQLException {
    TrxBalanceInfo info = (TrxBalanceInfo) item;
    preparedStatement.setString(index++, info.getFrozenBalance().toString());
    preparedStatement.setString(index++, info.getEnergyFrozenBalance().toString());
    preparedStatement.setString(index++, info.getDelegatedFrozenBalanceForEnergy().toString());
    preparedStatement.setString(index++, info.getDelegatedFrozenBalanceForBandwidth().toString());
    preparedStatement.setString(index++, info.getFrozenSupplyBalance().toString());
    preparedStatement.setString(index++, info.getAcquiredDelegatedFrozenBalanceForEnergy().toString());
    preparedStatement.setString(index++, info.getAcquiredDelegatedFrozenBalanceForBandwidth().toString());
  }

  private static final String updatSql = "update %s set balance =?, block_num =?, solidity_balance =?, solidity_block_num =?, decimals =?, updated_time =?  where id = ?";
  private static final String updatSql10 = "update %s set balance =?, block_num =?, solidity_balance =?, solidity_block_num =?, updated_time =?  where id = ?";
  private static final String updatSqlTrx = "update %s set balance =?, block_num =?, solidity_balance =?, solidity_block_num =?, " +
          " frozen_balance =?, energy_frozen_balance =?, delegated_frozen_balance_for_energy =?, delegated_frozen_balance_for_bandwidth =?, " +
          " frozen_supply_balance =?, acquired_delegated_frozen_balance_for_energy =?, acquired_delegated_frozen_balance_for_bandwidth =?," +
          " updated_time =?  where id = ?";


  private void update(Connection connection, List<BaseBalance> infos, int assetType) {
    try {
      if (CollectionUtils.isEmpty(infos)) {
        return;
      }

      infos.forEach(item -> {
        update(connection, item, assetType);
      });

//      preparedStatement.executeBatch();
    } catch (Exception e) {
      logger.error("",  e);
    }
  }

  private void update(Connection connection, BaseBalance item, int assetType) {
    if (item == null) {
      return;
    }

    try {
      PreparedStatement preparedStatement = getStatement(connection, UPDATE, assetType, item.getAccountAddress());
      int index = 1;
      preparedStatement.setString(index++, item.getBalance().toString());
      preparedStatement.setLong(index++, item.blockNum);
      preparedStatement.setString(index++, item.getBalance().toString());
      preparedStatement.setLong(index++, item.blockNum);

      if (assetType == TRX) {
        setBalance(item, index, preparedStatement);
        // index是基本类型，所以这里自增，需要额外处理
        index = index + 7;
      }
      else if (assetType == TRC10) {
        // do nothing
      }
      else if (assetType == TRC20) {
        BalanceInfo info = (BalanceInfo) item;
        preparedStatement.setString(index++, info.decimals.toString());
      }

      Timestamp now = Timestamp.valueOf(LocalDateTime.now());
      preparedStatement.setTimestamp(index++, now);
      preparedStatement.setLong(index++, item.id);

      preparedStatement.execute();
      preparedStatement.close();
    }
    catch (Exception e) {
      logger.error("", e);
    }
  }


  private void update(PreparedStatement preparedStatement, BaseBalance item, int assetType) {
    if (item == null) {
      return;
    }

    try {
      int index = 1;
      preparedStatement.setString(index++, item.getBalance().toString());
      preparedStatement.setLong(index++, item.blockNum);
      preparedStatement.setString(index++, item.getBalance().toString());
      preparedStatement.setLong(index++, item.blockNum);

      if (assetType == TRX) {
        setBalance(item, index, preparedStatement);
        // index是基本类型，所以这里自增，需要额外处理
        index = index + 7;
      }
      else if (assetType == TRC10) {
        // do nothing
      }
      else if (assetType == TRC20) {
        BalanceInfo info = (BalanceInfo) item;
        preparedStatement.setString(index++, info.decimals.toString());
      }

      Timestamp now = Timestamp.valueOf(LocalDateTime.now());
      preparedStatement.setTimestamp(index++, now);
      preparedStatement.setLong(index++, item.id);
      preparedStatement.addBatch();
    }
    catch (Exception e) {
      logger.error("", e);
    }
  }



  public static String redisHost = "127.0.0.1";
  public static Integer redisPort = 63791;
  public static String redisPass = "defi-redis";
  public static Integer redisDb = 1;

  private static Jedis jedis = null;

  private Jedis tryGetConn() {
    int reTry = 5;
    while (true && reTry >=0) {
      try {
        final Jedis conn = getConn();
        return conn;
      }
      catch (Exception ex) {
        reTry --;
      }
    }

    return null;
  }

  private synchronized Jedis getConn() {
    if (jedis != null && jedis.isConnected()) {
      return jedis;
    }

    Jedis jedis = new Jedis(redisHost, redisPort);
    if (!StringUtils.isEmpty(redisPass)) {
      jedis.auth(redisPass);
    }
    jedis.select(redisDb);
    return jedis;
  }

  public static final String INIT_KEY = "tron-link-data-init";
  public static final String BLOCK_CURRENT_NUM = "tron-link-current-num";
  public static final String BLOCK_CURRENT_HASH = "tron-link-current-hash";
  public static final String BLOCK_CURRENT_SOLIDITY_NUM = "tron-link-current-solidity-num";
  public static final String BLOCK_CURRENT_SOLIDITY_HASH = "tron-link-current-solidity-hash";

  public void syncDataToRedis(BlockCapsule blockCapsule) {
    try {
      final Jedis conn = getConn();
      conn.set(INIT_KEY, "true");
      conn.set(BLOCK_CURRENT_NUM, "" + blockCapsule.getNum());
      System.out.println(" >>>>>  save redis, num:" + blockCapsule.getNum());
      conn.set(BLOCK_CURRENT_HASH, "" + blockCapsule.getBlockId().toString());
      conn.set(BLOCK_CURRENT_SOLIDITY_NUM, "" + blockCapsule.getNum());
      conn.set(BLOCK_CURRENT_SOLIDITY_HASH, "" + blockCapsule.getBlockId().toString());
      System.out.println(" >>>>> syncDataToRedis success. num:" + blockCapsule.getNum());
    }
    catch (Exception ex) {
      System.out.println(" >>>> update redis error !!!");
      logger.error(" update redis error, num:" + blockCapsule.getNum(),  ex);
    }
  }
}
