package org.tron.core.services.http;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.tron.api.GrpcAPI.BlockList;
import org.tron.api.GrpcAPI.EasyTransferResponse;
import org.tron.api.GrpcAPI.TransactionApprovedList;
import org.tron.api.GrpcAPI.TransactionExtention;
import org.tron.api.GrpcAPI.TransactionList;
import org.tron.api.GrpcAPI.TransactionSignWeight;
import org.tron.common.crypto.Hash;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.config.args.Args;
import org.tron.core.services.http.JsonFormat.ParseException;
import org.tron.protos.Contract.AccountCreateContract;
import org.tron.protos.Contract.AccountPermissionUpdateContract;
import org.tron.protos.Contract.AccountUpdateContract;
import org.tron.protos.Contract.CreateSmartContract;
import org.tron.protos.Contract.FreezeBalanceContract;
import org.tron.protos.Contract.ProposalApproveContract;
import org.tron.protos.Contract.SideChainProposalCreateContract;
import org.tron.protos.Contract.ProposalDeleteContract;
import org.tron.protos.Contract.TransferContract;
import org.tron.protos.Contract.TriggerSmartContract;
import org.tron.protos.Contract.UnfreezeBalanceContract;
import org.tron.protos.Contract.UpdateEnergyLimitContract;
import org.tron.protos.Contract.UpdateSettingContract;
import org.tron.protos.Contract.VoteWitnessContract;
import org.tron.protos.Contract.WithdrawBalanceContract;
import org.tron.protos.Contract.WitnessCreateContract;
import org.tron.protos.Contract.WitnessUpdateContract;
import org.tron.protos.Protocol.Block;
import org.tron.protos.Protocol.SmartContract;
import org.tron.protos.Protocol.Transaction;


@Slf4j(topic = "API")
public class Util {

  public static String printErrorMsg(Exception e) {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("Error", e.getClass() + " : " + e.getMessage());
    return jsonObject.toJSONString();
  }

  public static String printBlockList(BlockList list) {
    List<Block> blocks = list.getBlockList();
    JSONObject jsonObject = JSONObject.parseObject(JsonFormat.printToString(list));
    JSONArray jsonArray = new JSONArray();
    blocks.stream().forEach(block -> {
      jsonArray.add(printBlockToJSON(block));
    });
    jsonObject.put("block", jsonArray);

    return jsonObject.toJSONString();
  }

  public static String printBlock(Block block) {
    return printBlockToJSON(block).toJSONString();
  }

  public static JSONObject printBlockToJSON(Block block) {
    BlockCapsule blockCapsule = new BlockCapsule(block);
    String blockID = ByteArray.toHexString(blockCapsule.getBlockId().getBytes());
    JSONObject jsonObject = JSONObject.parseObject(JsonFormat.printToString(block));
    jsonObject.put("blockID", blockID);
    if (!blockCapsule.getTransactions().isEmpty()) {
      jsonObject.put("transactions", printTransactionListToJSON(blockCapsule.getTransactions()));
    }
    return jsonObject;
  }

  public static String printTransactionList(TransactionList list) {
    List<Transaction> transactions = list.getTransactionList();
    JSONObject jsonObject = JSONObject.parseObject(JsonFormat.printToString(list));
    JSONArray jsonArray = new JSONArray();
    transactions.stream().forEach(transaction -> {
      jsonArray.add(printTransactionToJSON(transaction));
    });
    jsonObject.put("transaction", jsonArray);

    return jsonObject.toJSONString();
  }

  public static JSONArray printTransactionListToJSON(List<TransactionCapsule> list) {
    JSONArray transactions = new JSONArray();
    list.stream().forEach(transactionCapsule -> {
      transactions.add(printTransactionToJSON(transactionCapsule.getInstance()));
    });
    return transactions;
  }

  public static String printEasyTransferResponse(EasyTransferResponse response) {
    JSONObject jsonResponse = JSONObject.parseObject(JsonFormat.printToString(response));
    jsonResponse.put("transaction", printTransactionToJSON(response.getTransaction()));
    return jsonResponse.toJSONString();
  }

  public static String printTransaction(Transaction transaction) {
    return printTransactionToJSON(transaction).toJSONString();
  }

  public static String printTransactionExtention(TransactionExtention transactionExtention) {
    String string = JsonFormat.printToString(transactionExtention);
    JSONObject jsonObject = JSONObject.parseObject(string);
    if (transactionExtention.getResult().getResult()) {
      jsonObject.put("transaction", printTransactionToJSON(transactionExtention.getTransaction()));
    }
    return jsonObject.toJSONString();
  }

  public static String printTransactionSignWeight(TransactionSignWeight transactionSignWeight) {
    String string = JsonFormat.printToString(transactionSignWeight);
    JSONObject jsonObject = JSONObject.parseObject(string);
    JSONObject jsonObjectExt = jsonObject.getJSONObject("transaction");
    jsonObjectExt
        .put("transaction",
            printTransactionToJSON(transactionSignWeight.getTransaction().getTransaction()));
    jsonObject.put("transaction", jsonObjectExt);
    return jsonObject.toJSONString();
  }

  public static String printTransactionApprovedList(
      TransactionApprovedList transactionApprovedList) {
    String string = JsonFormat.printToString(transactionApprovedList);
    JSONObject jsonObject = JSONObject.parseObject(string);
    JSONObject jsonObjectExt = jsonObject.getJSONObject("transaction");
    jsonObjectExt
        .put("transaction",
            printTransactionToJSON(transactionApprovedList.getTransaction().getTransaction()));
    jsonObject.put("transaction", jsonObjectExt);
    return jsonObject.toJSONString();
  }

  public static byte[] generateContractAddress(Transaction trx, byte[] ownerAddress) {
    // get tx hash
    byte[] txRawDataHash = Sha256Hash.of(trx.getRawData().toByteArray()).getBytes();

    // combine
    byte[] combined = new byte[txRawDataHash.length + ownerAddress.length];
    System.arraycopy(txRawDataHash, 0, combined, 0, txRawDataHash.length);
    System.arraycopy(ownerAddress, 0, combined, txRawDataHash.length, ownerAddress.length);

    return Hash.sha3omit12(combined);
  }

  public static JSONObject printTransactionToJSON(Transaction transaction) {
    JSONObject jsonTransaction = JSONObject.parseObject(JsonFormat.printToString(transaction));
    JSONArray contracts = new JSONArray();
    transaction.getRawData().getContractList().stream().forEach(contract -> {
      try {
        JSONObject contractJson = null;
        Any contractParameter = contract.getParameter();
        switch (contract.getType()) {
          case AccountCreateContract:
            AccountCreateContract accountCreateContract = contractParameter
                .unpack(AccountCreateContract.class);
            contractJson = JSONObject.parseObject(JsonFormat.printToString(accountCreateContract));
            break;
          case TransferContract:
            TransferContract transferContract = contractParameter.unpack(TransferContract.class);
            contractJson = JSONObject.parseObject(JsonFormat.printToString(transferContract));
            break;
          case VoteWitnessContract:
            VoteWitnessContract voteWitnessContract = contractParameter
                .unpack(VoteWitnessContract.class);
            contractJson = JSONObject.parseObject(JsonFormat.printToString(voteWitnessContract));
            break;
          case WitnessCreateContract:
            WitnessCreateContract witnessCreateContract = contractParameter
                .unpack(WitnessCreateContract.class);
            contractJson = JSONObject.parseObject(JsonFormat.printToString(witnessCreateContract));
            break;
          case WitnessUpdateContract:
            WitnessUpdateContract witnessUpdateContract = contractParameter
                .unpack(WitnessUpdateContract.class);
            contractJson = JSONObject.parseObject(JsonFormat.printToString(witnessUpdateContract));
            break;
          case AccountUpdateContract:
            AccountUpdateContract accountUpdateContract = contractParameter
                .unpack(AccountUpdateContract.class);
            contractJson = JSONObject.parseObject(JsonFormat.printToString(accountUpdateContract));
            break;
          case FreezeBalanceContract:
            FreezeBalanceContract freezeBalanceContract = contractParameter
                .unpack(FreezeBalanceContract.class);
            contractJson = JSONObject.parseObject(JsonFormat.printToString(freezeBalanceContract));
            break;
          case UnfreezeBalanceContract:
            UnfreezeBalanceContract unfreezeBalanceContract = contractParameter
                .unpack(UnfreezeBalanceContract.class);
            contractJson = JSONObject
                .parseObject(JsonFormat.printToString(unfreezeBalanceContract));
            break;
          case WithdrawBalanceContract:
            WithdrawBalanceContract withdrawBalanceContract = contractParameter
                .unpack(WithdrawBalanceContract.class);
            contractJson = JSONObject
                .parseObject(JsonFormat.printToString(withdrawBalanceContract));
            break;
          case CreateSmartContract:
            CreateSmartContract deployContract = contractParameter
                .unpack(CreateSmartContract.class);
            contractJson = JSONObject.parseObject(JsonFormat.printToString(deployContract));
            byte[] ownerAddress = deployContract.getOwnerAddress().toByteArray();
            byte[] contractAddress = generateContractAddress(transaction, ownerAddress);
            jsonTransaction.put("contract_address", ByteArray.toHexString(contractAddress));
            break;
          case TriggerSmartContract:
            TriggerSmartContract triggerSmartContract = contractParameter
                .unpack(TriggerSmartContract.class);
            contractJson = JSONObject.parseObject(JsonFormat.printToString(triggerSmartContract));
            break;
          case SideChainProposalCreateContract:
            SideChainProposalCreateContract proposalCreateContract = contractParameter
                .unpack(SideChainProposalCreateContract.class);
            contractJson = JSONObject.parseObject(JsonFormat.printToString(proposalCreateContract));
            break;
          case ProposalApproveContract:
            ProposalApproveContract proposalApproveContract = contractParameter
                .unpack(ProposalApproveContract.class);
            contractJson = JSONObject
                .parseObject(JsonFormat.printToString(proposalApproveContract));
            break;
          case ProposalDeleteContract:
            ProposalDeleteContract proposalDeleteContract = contractParameter
                .unpack(ProposalDeleteContract.class);
            contractJson = JSONObject.parseObject(JsonFormat.printToString(proposalDeleteContract));
            break;
          case AccountPermissionUpdateContract:
            AccountPermissionUpdateContract accountPermissionUpdateContract = contractParameter
                .unpack(AccountPermissionUpdateContract.class);
            contractJson = JSONObject
                .parseObject(JsonFormat.printToString(accountPermissionUpdateContract));
            break;
          case UpdateSettingContract:
            UpdateSettingContract updateSettingContract = contractParameter
                .unpack(UpdateSettingContract.class);
            contractJson = JSONObject.parseObject(JsonFormat.printToString(updateSettingContract));
            break;
          case UpdateEnergyLimitContract:
            UpdateEnergyLimitContract updateEnergyLimitContract = contractParameter
                .unpack(UpdateEnergyLimitContract.class);
            contractJson = JSONObject
                .parseObject(JsonFormat.printToString(updateEnergyLimitContract));
            break;
          // todo add other contract
          default:
        }
        JSONObject parameter = new JSONObject();
        parameter.put("value", contractJson);
        parameter.put("type_url", contract.getParameterOrBuilder().getTypeUrl());
        JSONObject jsonContract = new JSONObject();
        jsonContract.put("parameter", parameter);
        jsonContract.put("type", contract.getType());
        contracts.add(jsonContract);
      } catch (InvalidProtocolBufferException e) {
        logger.debug("InvalidProtocolBufferException: {}", e.getMessage());
      }
    });

    JSONObject rawData = JSONObject.parseObject(jsonTransaction.get("raw_data").toString());
    rawData.put("contract", contracts);
    jsonTransaction.put("raw_data", rawData);
    String rawDataHex = ByteArray.toHexString(transaction.getRawData().toByteArray());
    jsonTransaction.put("raw_data_hex", rawDataHex);
    String txID = ByteArray.toHexString(Sha256Hash.hash(transaction.getRawData().toByteArray()));
    jsonTransaction.put("txID", txID);
    return jsonTransaction;
  }

  public static Transaction packTransaction(String strTransaction) {
    JSONObject jsonTransaction = JSONObject.parseObject(strTransaction);
    JSONObject rawData = jsonTransaction.getJSONObject("raw_data");
    JSONArray contracts = new JSONArray();
    JSONArray rawContractArray = rawData.getJSONArray("contract");

    for (int i = 0; i < rawContractArray.size(); i++) {
      try {
        JSONObject contract = rawContractArray.getJSONObject(i);
        JSONObject parameter = contract.getJSONObject("parameter");
        String contractType = contract.getString("type");
        Any any = null;
        switch (contractType) {
          case "AccountCreateContract":
            AccountCreateContract.Builder accountCreateContractBuilder = AccountCreateContract
                .newBuilder();
            JsonFormat.merge(parameter.getJSONObject("value").toJSONString(),
                accountCreateContractBuilder);
            any = Any.pack(accountCreateContractBuilder.build());
            break;
          case "TransferContract":
            TransferContract.Builder transferContractBuilder = TransferContract.newBuilder();
            JsonFormat
                .merge(parameter.getJSONObject("value").toJSONString(), transferContractBuilder);
            any = Any.pack(transferContractBuilder.build());
            break;
          case "VoteWitnessContract":
            VoteWitnessContract.Builder voteWitnessContractBuilder = VoteWitnessContract
                .newBuilder();
            JsonFormat
                .merge(parameter.getJSONObject("value").toJSONString(), voteWitnessContractBuilder);
            any = Any.pack(voteWitnessContractBuilder.build());
            break;
          case "WitnessCreateContract":
            WitnessCreateContract.Builder witnessCreateContractBuilder = WitnessCreateContract
                .newBuilder();
            JsonFormat.merge(parameter.getJSONObject("value").toJSONString(),
                witnessCreateContractBuilder);
            any = Any.pack(witnessCreateContractBuilder.build());
            break;
          case "WitnessUpdateContract":
            WitnessUpdateContract.Builder witnessUpdateContractBuilder = WitnessUpdateContract
                .newBuilder();
            JsonFormat.merge(parameter.getJSONObject("value").toJSONString(),
                witnessUpdateContractBuilder);
            any = Any.pack(witnessUpdateContractBuilder.build());
            break;
          case "AccountUpdateContract":
            AccountUpdateContract.Builder accountUpdateContractBuilder = AccountUpdateContract
                .newBuilder();
            JsonFormat.merge(parameter.getJSONObject("value").toJSONString(),
                accountUpdateContractBuilder);
            any = Any.pack(accountUpdateContractBuilder.build());
            break;
          case "FreezeBalanceContract":
            FreezeBalanceContract.Builder freezeBalanceContractBuilder = FreezeBalanceContract
                .newBuilder();
            JsonFormat.merge(parameter.getJSONObject("value").toJSONString(),
                freezeBalanceContractBuilder);
            any = Any.pack(freezeBalanceContractBuilder.build());
            break;
          case "UnfreezeBalanceContract":
            UnfreezeBalanceContract.Builder unfreezeBalanceContractBuilder = UnfreezeBalanceContract
                .newBuilder();
            JsonFormat.merge(parameter.getJSONObject("value").toJSONString(),
                unfreezeBalanceContractBuilder);
            any = Any.pack(unfreezeBalanceContractBuilder.build());
            break;
          case "WithdrawBalanceContract":
            WithdrawBalanceContract.Builder withdrawBalanceContractBuilder = WithdrawBalanceContract
                .newBuilder();
            JsonFormat.merge(parameter.getJSONObject("value").toJSONString(),
                withdrawBalanceContractBuilder);
            any = Any.pack(withdrawBalanceContractBuilder.build());
            break;
          case "SmartContract":
            SmartContract.Builder smartContractBuilder = SmartContract.newBuilder();
            JsonFormat
                .merge(parameter.getJSONObject("value").toJSONString(), smartContractBuilder);
            any = Any.pack(smartContractBuilder.build());
            break;
          case "TriggerSmartContract":
            TriggerSmartContract.Builder triggerSmartContractBuilder = TriggerSmartContract
                .newBuilder();
            JsonFormat
                .merge(parameter.getJSONObject("value").toJSONString(),
                    triggerSmartContractBuilder);
            any = Any.pack(triggerSmartContractBuilder.build());
            break;
          case "CreateSmartContract":
            CreateSmartContract.Builder createSmartContractBuilder = CreateSmartContract
                .newBuilder();
            JsonFormat
                .merge(parameter.getJSONObject("value").toJSONString(),
                    createSmartContractBuilder);
            any = Any.pack(createSmartContractBuilder.build());
            break;
          case "SideChainProposalCreateContract":
            SideChainProposalCreateContract.Builder SideChainProposalCreateContractBuilder = SideChainProposalCreateContract
                .newBuilder();
            JsonFormat
                .merge(parameter.getJSONObject("value").toJSONString(),
                    SideChainProposalCreateContractBuilder);
            any = Any.pack(SideChainProposalCreateContractBuilder.build());
            break;
          case "ProposalApproveContract":
            ProposalApproveContract.Builder ProposalApproveContractBuilder = ProposalApproveContract
                .newBuilder();
            JsonFormat
                .merge(parameter.getJSONObject("value").toJSONString(),
                    ProposalApproveContractBuilder);
            any = Any.pack(ProposalApproveContractBuilder.build());
            break;
          case "ProposalDeleteContract":
            ProposalDeleteContract.Builder ProposalDeleteContractBuilder = ProposalDeleteContract
                .newBuilder();
            JsonFormat
                .merge(parameter.getJSONObject("value").toJSONString(),
                    ProposalDeleteContractBuilder);
            any = Any.pack(ProposalDeleteContractBuilder.build());
            break;
          case "AccountPermissionUpdateContract":
            AccountPermissionUpdateContract.Builder AccountPermissionUpdateContractBuilder =
                AccountPermissionUpdateContract.newBuilder();
            JsonFormat
                .merge(parameter.getJSONObject("value").toJSONString(),
                    AccountPermissionUpdateContractBuilder);
            any = Any.pack(AccountPermissionUpdateContractBuilder.build());
            break;
          case "UpdateSettingContract":
            UpdateSettingContract.Builder UpdateSettingContractBuilder = UpdateSettingContract
                .newBuilder();
            JsonFormat
                .merge(parameter.getJSONObject("value").toJSONString(),
                    UpdateSettingContractBuilder);
            any = Any.pack(UpdateSettingContractBuilder.build());
            break;
          case "UpdateEnergyLimitContract":
            UpdateEnergyLimitContract.Builder UpdateEnergyLimitContractBuilder = UpdateEnergyLimitContract
                .newBuilder();
            JsonFormat
                .merge(parameter.getJSONObject("value").toJSONString(),
                    UpdateEnergyLimitContractBuilder);
            any = Any.pack(UpdateEnergyLimitContractBuilder.build());
            break;
          // todo add other contract
          default:
        }
        if (any != null) {
          String value = ByteArray.toHexString(any.getValue().toByteArray());
          parameter.put("value", value);
          contract.put("parameter", parameter);
          contracts.add(contract);
        }
      } catch (ParseException e) {
        logger.debug("ParseException: {}", e.getMessage());
      }
    }
    rawData.put("contract", contracts);
    jsonTransaction.put("raw_data", rawData);
    Transaction.Builder transactionBuilder = Transaction.newBuilder();
    try {
      JsonFormat.merge(jsonTransaction.toJSONString(), transactionBuilder);
      return transactionBuilder.build();
    } catch (ParseException e) {
      logger.debug("ParseException: {}", e.getMessage());
      return null;
    }
  }

  public static void checkBodySize(String body) throws Exception {
    Args args = Args.getInstance();
    if (body.getBytes().length > args.getMaxMessageSize()) {
      throw new Exception("body size is too big, limit is " + args.getMaxMessageSize());
    }
  }
}