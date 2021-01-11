package org.tron.program;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import java.io.File;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;
import org.spongycastle.util.encoders.Hex;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.tron.common.application.Application;
import org.tron.common.application.ApplicationFactory;
import org.tron.common.application.ApplicationHandler;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.runtime.ProgramResult;
import org.tron.common.runtime.vm.DataWord;
import org.tron.common.utils.*;
import org.tron.core.Constant;
import org.tron.core.actuator.VMActuator;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.capsule.TransactionRetCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.db.*;
import org.tron.core.exception.BadItemException;
import org.tron.core.store.AccountStore;
import org.tron.core.store.StoreFactory;
import org.tron.core.vm.utils.MUtil;
import org.tron.protos.Protocol;
import org.tron.protos.contract.SmartContractOuterClass;

@Slf4j(topic = "app")
public class FullNode {

  public static void load(String path) {
    try {
      File file = new File(path);
      if (!file.exists() || !file.isFile() || !file.canRead()) {
        return;
      }
      LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
      JoranConfigurator configurator = new JoranConfigurator();
      configurator.setContext(lc);
      lc.reset();
      configurator.doConfigure(file);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  private static TransactionRetStore transactionRetStore;
  private static BlockStore blockStore;
  private static BlockIndexStore blockIndexStore;
  private static AccountStore accountStore;
  static final String WTRXAddress = "TNUC9Qb1rRpS5CbWLmNMxXBjyFoydXjWFR";

  /**
   * Start the FullNode.
   */
  public static void main(String[] args) {
    logger.info("Full node running.");
    Args.setParam(args, Constant.TESTNET_CONF);
    Args cfgArgs = Args.getInstance();

    load(cfgArgs.getLogbackPath());

    if (cfgArgs.isHelp()) {
      logger.info("Here is the help message.");
      return;
    }

    if (Args.getInstance().isDebug()) {
      logger.info("in debug mode, it won't check energy time");
    } else {
      logger.info("not in debug mode, it will check energy time");
    }

    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.setAllowCircularReferences(false);
    TronApplicationContext context =
        new TronApplicationContext(beanFactory);
    context.register(DefaultConfig.class);

    context.refresh();
    Application appT = ApplicationFactory.create(context);
    shutdown(appT);

    final Manager dbManager = appT.getDbManager();
    blockStore = dbManager.getBlockStore();
    blockIndexStore = dbManager.getBlockIndexStore();
    transactionRetStore = dbManager.getTransactionRetStore();
    accountStore = dbManager.getAccountStore();

    final long headBlockNum = dbManager.getHeadBlockNum();
    System.out.println(" >>>>>>>>>>> headBlockNum" + headBlockNum);
    handlerBalance(headBlockNum);
    handlerTrc20(headBlockNum);
    final BlockCapsule blockCapsule = getBlockByNum(headBlockNum);
    System.out.println(" >>>> syncDataToRedis start");
    handlerList.get(0).syncDataToRedis(blockCapsule);

    while (!allOver()) {
      try {
        Thread.sleep(1000);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }

    System.out.println(" >>>>>>>>>>> main is end!!!!!!!!");
    ApplicationHandler.closeSelf();
    System.exit(0);
  }

  private static boolean allOver() {
    return handlerList.stream().allMatch(item -> item.isOver());
  }

  private static void handlerTrc20(long headBlockNum) {
    //避免trigger超时
    DBConfig.setDebug(true);

    long l1 = System.currentTimeMillis();
    Map<String, Set<String>> tokenMap = new ConcurrentHashMap<>(8);
    handlerMap(headBlockNum, tokenMap);
    System.out.println(" >>> tokenMap.size:{}" + tokenMap.keySet().size());

    final long sum = tokenMap.values().stream().mapToLong(Set::size).sum();
    long l2 = System.currentTimeMillis();
    System.out.println(" >>> tokenMap.size:{}" + sum + ", cost:" + (l2 - l1));

    l1 = System.currentTimeMillis();
    handlerMapToDb(headBlockNum, tokenMap);
    l2 = System.currentTimeMillis();
    System.out.println(" >>> handlerMapToDB, cost:{}" + (l2 - l1));
  }

  private static final ConcurrentLinkedQueue<SyncDataToDB.BaseBalance> TRX_LIST = new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<SyncDataToDB.BaseBalance> TRC10_LIST = new ConcurrentLinkedQueue<>();

  private static final List<SyncDataToDB> handlerList = new ArrayList<>(3);


  private static void handlerBalance(long blockNum) {
    final SyncDataToDB trxToDB = new SyncDataToDB(TRX_LIST, SyncDataToDB.TRX);
    trxToDB.start();
    handlerList.add(trxToDB);
    final SyncDataToDB trc10ToDB = new SyncDataToDB(TRC10_LIST, SyncDataToDB.TRC10);
    trc10ToDB.start();
    handlerList.add(trc10ToDB);

    AtomicLong count = new AtomicLong(0);

    for (Map.Entry<byte[], AccountCapsule> accountCapsuleEntry : accountStore) {
      final AccountCapsule accountCapsule = accountCapsuleEntry.getValue();
      final String address = WalletUtil.encode58Check(accountCapsule.getAddress().toByteArray());

      TRX_LIST.add(convert(accountCapsule, address, blockNum));
      TRC10_LIST.addAll(convertTrc10(accountCapsule, address, blockNum));

      final long l = count.incrementAndGet();

      if (l % 10000 == 0) {
        logger.info(" >>>> handlerBalance:{}", l);
      }
    }

    trxToDB.setOver();
    trc10ToDB.setOver();
  }

  private static SyncDataToDB.TrxBalanceInfo convert(AccountCapsule accountCapsule, String address,
                                                     long blockNum) {
    SyncDataToDB.TrxBalanceInfo trx = new SyncDataToDB.TrxBalanceInfo();
    trx.setAccountAddress(address);
    trx.setBlockNum(blockNum);
    trx.setBalance(BigInteger.valueOf(accountCapsule.getBalance()));
    trx.setFrozenBalance(BigInteger.valueOf(accountCapsule.getFrozenBalance()));
    trx.setEnergyFrozenBalance(BigInteger.valueOf(accountCapsule.getEnergyFrozenBalance()));
    trx.setDelegatedFrozenBalanceForEnergy(
            BigInteger.valueOf(accountCapsule.getDelegatedFrozenBalanceForEnergy()));
    trx.setDelegatedFrozenBalanceForBandwidth(
            BigInteger.valueOf(accountCapsule.getDelegatedFrozenBalanceForBandwidth()));
    trx.setFrozenSupplyBalance(BigInteger.valueOf(accountCapsule.getFrozenSupplyBalance()));
    trx.setAcquiredDelegatedFrozenBalanceForEnergy(
            BigInteger.valueOf(accountCapsule.getAcquiredDelegatedFrozenBalanceForEnergy()));
    trx.setAcquiredDelegatedFrozenBalanceForBandwidth(
            BigInteger.valueOf(accountCapsule.getAcquiredDelegatedFrozenBalanceForBandwidth()));
    return trx;
  }

  private static List<SyncDataToDB.Trc10BalanceInfo> convertTrc10(AccountCapsule accountCapsule,
                                                                  String address, long blockNum) {
    return accountCapsule.getAssetMapV2()
            .entrySet().stream().map(item -> {
              SyncDataToDB.Trc10BalanceInfo trc10 = new SyncDataToDB.Trc10BalanceInfo();
              trc10.setAccountAddress(address);
              trc10.setBlockNum(blockNum);
              trc10.setTokenId(item.getKey());
              trc10.setBalance(BigInteger.valueOf(item.getValue()));
              return trc10;
            }).collect(Collectors.toList());
  }


  private static void handlerMapToDb(long headBlockNum, Map<String, Set<String>> tokenMap) {
    final BlockCapsule blockCapsule = getBlockByNum(headBlockNum);
    final AtomicInteger count = new AtomicInteger();

    final ConcurrentLinkedQueue<SyncDataToDB.BaseBalance> queue = new ConcurrentLinkedQueue<>();
    final SyncDataToDB trc20ToDB = new SyncDataToDB(queue, SyncDataToDB.TRC20);
    trc20ToDB.start();
    handlerList.add(trc20ToDB);
    // 并行流triggerVM会报错，这里不使用并行流
    tokenMap.forEach((tokenAddress, accountAddressSet) -> {
      try {
        // 并行流triggerVM会报错，这里不使用并行流
        accountAddressSet.forEach(accountAddress -> {
          BigInteger oldTrc20Decimal = getTRC20Decimal(tokenAddress, blockCapsule, accountAddress);
          final BigInteger trc20Decimal = oldTrc20Decimal == null ? BigInteger.ZERO : oldTrc20Decimal;
          BigInteger trc20Balance = getTRC20Balance(accountAddress, tokenAddress, blockCapsule);
          trc20Balance = trc20Balance == null ? BigInteger.ZERO : trc20Balance;
          final SyncDataToDB.BalanceInfo info = new SyncDataToDB.BalanceInfo();
          info.setTokenAddress(tokenAddress);
          info.setAccountAddress(accountAddress);
          info.setBalance(trc20Balance);
          info.setBlockNum(headBlockNum);
          info.setDecimals(trc20Decimal.intValue());
          queue.add(info);

          if (count.incrementAndGet() % 10000 == 0) {
            System.out.println(
                    " >>> token:" + tokenAddress + ", dec:" + trc20Decimal + ", time:" + System
                            .currentTimeMillis());
          }
        });
      } catch (Exception ex) {
        logger.error("", ex);
      }
    });

    trc20ToDB.setOver();
  }

  private static void handlerMap(long headBlockNum, Map<String, Set<String>> tokenMap) {
    LongStream.range(0, headBlockNum + 1).parallel().forEach(num -> {
      parseTrc20Map(num, tokenMap);

      if (num % (10 * 10000) == 0) {
        System.out.println(" >>>>>>>>>>> handlerMap, num:" + num);
      }
    });
  }

  private static BlockCapsule getBlockByNum(long num) {
    BlockCapsule blockCapsule = null;
    try {
      blockCapsule = blockStore.get(blockIndexStore.get(num).getBytes());
    } catch (Exception e) {
      logger.error(" >>> get block error, num:{}", num);
    }
    return blockCapsule;
  }

  public static void parseTrc20Map(Long blockNum, Map<String, Set<String>> tokenMap) {
    try {
      TransactionRetCapsule retCapsule = transactionRetStore
              .getTransactionInfoByBlockNum(ByteArray.fromLong(blockNum));
      if (retCapsule != null) {
        retCapsule.getInstance().getTransactioninfoList().parallelStream().forEach(item -> {
          List<Protocol.TransactionInfo.Log> logs = item.getLogList();
          logs.parallelStream().forEach(l -> handlerToMap(l, tokenMap));
        });
      }
    } catch (BadItemException e) {
      logger.error("TRC20Parser: block: {} parse error ", blockNum);
    }
  }

  private static void handlerToMap(Protocol.TransactionInfo.Log log,
                                   Map<String, Set<String>> tokenMap) {
    final List<ByteString> topicsList = log.getTopicsList();

    if (CollectionUtils.isEmpty(topicsList) || topicsList.size() < 2) {
      return;
    }

    final String topic0 = new DataWord(topicsList.get(0).toByteArray()).toHexString();
    String tokenAddress = WalletUtil
            .encode58Check(MUtil.convertToTronAddress(log.getAddress().toByteArray()));

    switch (ConcernTopics.getBySH(topic0)) {
      case TRANSFER:
        if (topicsList.size() < 3) {
          return;
        }
        String senderAddr = MUtil.encode58Check(MUtil
                .convertToTronAddress(new DataWord(topicsList.get(1).toByteArray()).getLast20Bytes()));
        String recAddr = MUtil.encode58Check(MUtil
                .convertToTronAddress(new DataWord(topicsList.get(2).toByteArray()).getLast20Bytes()));
        Set<String> accountAddressSet = tokenMap
                .computeIfAbsent(tokenAddress, k -> ConcurrentHashMap.newKeySet());
        accountAddressSet.add(senderAddr);
        accountAddressSet.add(recAddr);
        break;
      case Deposit:
      case Withdrawal:
        if (!tokenAddress.equals(WTRXAddress) || topicsList.size() < 2) {
          return;
        }
        accountAddressSet = tokenMap
                .computeIfAbsent(tokenAddress, k -> ConcurrentHashMap.newKeySet());
        String accountAddr = MUtil.encode58Check(MUtil
                .convertToTronAddress(new DataWord(topicsList.get(1).toByteArray()).getLast20Bytes()));
        accountAddressSet.add(accountAddr);
        break;
      default:
        return;
    }
  }

  private static BigInteger getTRC20Balance(String ownerAddress, String contractAddress,
                                            BlockCapsule baseBlockCap) {
    byte[] data = Bytes.concat(Hex.decode("70a082310000000000000000000000"),
            Commons.decodeFromBase58Check(ownerAddress));
    ProgramResult result = triggerFromVM(contractAddress, data, baseBlockCap, ownerAddress);
    if (result != null
            && !result.isRevert() && StringUtils.isEmpty(result.getRuntimeError())
            && result.getHReturn() != null) {
      try {
        final BigInteger bigInteger = toBigInteger(result.getHReturn());
        logger.info(" >>>>> {} getTRC20Balance success {}", contractAddress, bigInteger);
        return bigInteger;
      } catch (Exception e) {
        logger.error("", e);
      }
    }
    return null;
  }

  private static BigInteger getTRC20Decimal(String contractAddress, BlockCapsule baseBlockCap, String owner) {
    byte[] data = Hex.decode("313ce567");
    ProgramResult result = triggerFromVM(contractAddress, data, baseBlockCap, owner);
    if (result != null
            && !result.isRevert() && StringUtils.isEmpty(result.getRuntimeError())
            && result.getHReturn() != null) {
      try {
        final BigInteger bigInteger = toBigInteger(result.getHReturn());
        logger.info(" >>>>> {} getTRC20Decimal success {}", contractAddress, bigInteger);
        return bigInteger;
      } catch (Exception e) {
        logger.error("", e);
      }
    }
    return null;
  }

  private static VMActuator vmActuator = new VMActuator(true);



  private static ProgramResult triggerFromVM(String contractAddress, byte[] data,
                                             BlockCapsule baseBlockCap, String owner) {
    SmartContractOuterClass.TriggerSmartContract.Builder build = SmartContractOuterClass.TriggerSmartContract.newBuilder();
    build.setData(ByteString.copyFrom(data));
    build.setOwnerAddress(ByteString.copyFrom(Commons.decodeFromBase58Check("T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb")));
    build.setCallValue(0);
    build.setCallTokenValue(0);
    build.setTokenId(0);
    build.setContractAddress(ByteString.copyFrom(Commons.decodeFromBase58Check(contractAddress)));
    TransactionCapsule trx = new TransactionCapsule(build.build(),
            Protocol.Transaction.Contract.ContractType.TriggerSmartContract);
    Protocol.Transaction.Builder txBuilder = trx.getInstance().toBuilder();
    Protocol.Transaction.raw.Builder rawBuilder = trx.getInstance().getRawData().toBuilder();
    rawBuilder.setFeeLimit(1000000000L);
    txBuilder.setRawData(rawBuilder);

    TransactionContext context = new TransactionContext(baseBlockCap,
            new TransactionCapsule(txBuilder.build()),
            StoreFactory.getInstance(), true,
            false);

    try {
      vmActuator.validate(context);
      vmActuator.execute(context);
    } catch (Exception e) {
      logger.warn("{} trigger failed!", contractAddress);
      logger.error("", e);
    }

    ProgramResult result = context.getProgramResult();
    return result;
  }

  private static BigInteger toBigInteger(byte[] input) {
    logger.info(" >>>> input:{}", Arrays.toString(input));
    if (input != null && input.length > 0) {
      try {
        if (input.length > 32) {
          input = Arrays.copyOfRange(input, 0, 32);
        }

        String hex = Hex.toHexString(input);
        logger.info(" >>>> hex : {}", hex);
        return hexStrToBigInteger(hex);
      } catch (Exception e) {
        logger.error("", e);
      }
    }
    return null;
  }

  private static BigInteger hexStrToBigInteger(String hexStr) {
    if (!StringUtils.isEmpty(hexStr)) {
      try {
        return new BigInteger(hexStr, 16);
      } catch (Exception e) {
        logger.error("", e);
      }
    }
    return null;
  }

  private enum ConcernTopics {
    TRANSFER("Transfer(address,address,uint256)",
            "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
    Withdrawal("Withdrawal(address,uint256)",
            "7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"),
    Deposit("Deposit(address,uint256)",
            "e1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"),
    UNKNOWN("UNKNOWN()",
            "0c78932dd210147f42a4ec6c5a353697626c4043d49be5f063518e57f3399e61");

    @Getter
    private String sign;
    @Getter
    private String signHash;


    ConcernTopics(String sign, String signHash) {
      this.sign = sign;
      this.signHash = signHash;
    }

    public static ConcernTopics getBySH(String signHa) {
      for (ConcernTopics value : ConcernTopics.values()) {
        if (value.signHash.equals(signHa)) {
          return value;
        }
      }
      return UNKNOWN;
    }
  }


  public static void shutdown(final Application app) {
    logger.info("********register application shutdown hook********");
    Runtime.getRuntime().addShutdownHook(new Thread(app::shutdown));
  }
}
