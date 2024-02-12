from hexbytes import HexBytes

from ape_subsquid.gateway import BlockHeader, Log, Transaction
from ape_subsquid.utils import hex_to_int


def map_header(value: BlockHeader, transactions: list[Transaction]) -> dict:
    return {
        "number": value["number"],
        "hash": HexBytes(value["hash"]),
        "parentHash": HexBytes(value["parentHash"]),
        "baseFeePerGas": value["baseFeePerGas"] and hex_to_int(value["baseFeePerGas"]),
        "difficulty": hex_to_int(value["difficulty"]),
        "totalDifficulty": hex_to_int(value["totalDifficulty"]),
        "extraData": HexBytes(value["extraData"]),
        "gasLimit": hex_to_int(value["gasLimit"]),
        "gasUsed": hex_to_int(value["gasUsed"]),
        "logsBloom": HexBytes(value["logsBloom"]),
        "miner": value["miner"],
        "mixHash": HexBytes(value["mixHash"]),
        "nonce": HexBytes(value["nonce"]),
        "receiptsRoot": HexBytes(value["receiptsRoot"]),
        "sha3Uncles": HexBytes(value["sha3Uncles"]),
        "size": value["size"],
        "stateRoot": HexBytes(value["stateRoot"]),
        "timestamp": int(value["timestamp"]),
        "transactionsRoot": HexBytes(value["transactionsRoot"]),
        "transactions": transactions,
    }


def map_receipt(
    value: Transaction,
    block_number: int,
    block_hash: HexBytes,
    logs: list[dict],
) -> dict:
    return {
        "blockNumber": block_number,
        "blockHash": block_hash,
        "from": value["from"],
        "to": value["to"],
        "hash": HexBytes(value["hash"]),
        "status": value["status"],
        "chainId": value["chainId"],
        "contractAddress": value["contractAddress"],
        "cumulativeGasUsed": hex_to_int(value["cumulativeGasUsed"]),
        "effectiveGasPrice": hex_to_int(value["effectiveGasPrice"]),
        "gas": hex_to_int(value["gas"]),
        "gasPrice": hex_to_int(value["gasPrice"]),
        "gasUsed": hex_to_int(value["gasUsed"]),
        "input": HexBytes(value["input"]),
        "maxFeePerGas": value["maxFeePerGas"] and hex_to_int(value["maxFeePerGas"]),
        "maxPriorityFeePerGas": value["maxPriorityFeePerGas"]
        and hex_to_int(value["maxPriorityFeePerGas"]),
        "nonce": value["nonce"],
        "v": hex_to_int(value["v"]),
        "r": HexBytes(value["r"]),
        "s": HexBytes(value["s"]),
        "transactionIndex": value["transactionIndex"],
        "type": value["type"],
        "value": hex_to_int(value["value"]),
        "yParity": value["yParity"],
        "transactionHash": HexBytes(value["hash"]),
        "logs": logs,
    }


def map_log(value: Log, block_number: int, block_hash: HexBytes) -> dict:
    return {
        "blockNumber": block_number,
        "blockHash": block_hash,
        "address": value["address"],
        "transactionIndex": value["transactionIndex"],
        "transactionHash": HexBytes(value["transactionHash"]),
        "logIndex": value["logIndex"],
        "data": HexBytes(value["data"]),
        "topics": [HexBytes(topic) for topic in value["topics"]],
    }
