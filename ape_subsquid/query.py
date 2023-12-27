from typing import Iterator, Optional

from ape.api import BlockAPI, ReceiptAPI
from ape.api.query import (
    AccountTransactionQuery,
    BlockQuery,
    ContractCreationQuery,
    ContractEventQuery,
    QueryAPI,
    QueryType,
)
from ape.exceptions import QueryEngineError
from ape.types import ContractLog
from ape.utils import singledispatchmethod
from ethpm_types import HexBytes

from ape_subsquid.archive import Archive, BlockHeader, Log, Transaction


def map_header(value: BlockHeader, transactions: list[Transaction]) -> dict:
    return {
        "number": value["number"],
        "hash": HexBytes(value["hash"]),
        "parentHash": HexBytes(value["parentHash"]),
        "baseFeePerGas": value["baseFeePerGas"] and int(value["baseFeePerGas"], 16),
        "difficulty": int(value["difficulty"], 16),
        "totalDifficulty": int(value["totalDifficulty"], 16),
        "extraData": HexBytes(value["extraData"]),
        "gasLimit": int(value["gasLimit"], 16),
        "gasUsed": int(value["gasUsed"], 16),
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
        "cumulativeGasUsed": int(value["cumulativeGasUsed"], 16),
        "effectiveGasPrice": int(value["effectiveGasPrice"], 16),
        "gas": int(value["gas"], 16),
        "gasPrice": int(value["gasPrice"], 16),
        "gasUsed": int(value["gasUsed"], 16),
        "input": HexBytes(value["input"]),
        "maxFeePerGas": value["maxFeePerGas"] and int(value["maxFeePerGas"], 16),
        "maxPriorityFeePerGas": value["maxPriorityFeePerGas"]
        and int(value["maxPriorityFeePerGas"], 16),
        "nonce": value["nonce"],
        "v": int(value["v"], 16),
        "r": HexBytes(value["r"]),
        "s": HexBytes(value["s"]),
        "transactionIndex": value["transactionIndex"],
        "type": value["type"],
        "value": int(value["value"], 16),
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


class SubsquidQueryEngine(QueryAPI):
    _archive = Archive()

    @singledispatchmethod
    def estimate_query(self, query: QueryType) -> Optional[int]:
        return None

    @estimate_query.register
    def estimate_block_query(self, query: BlockQuery) -> int:
        return 0

    @estimate_query.register
    def estimate_account_transaction_query(self, query: AccountTransactionQuery) -> int:
        return 0

    @estimate_query.register
    def estimate_contract_creation_query(self, query: ContractCreationQuery) -> int:
        return 0

    @estimate_query.register
    def estimate_contract_event_query(self, query: ContractEventQuery) -> int:
        return 0

    @singledispatchmethod
    def perform_query(self, query: QueryType) -> Iterator:
        raise QueryEngineError(
            f"{self.__class__.__name__} cannot handle {query.__class__.__name__} queries."
        )

    @perform_query.register
    def perform_block_query(self, query: BlockQuery) -> Iterator[BlockAPI]:
        from_block = query.start_block
        while True:
            data = self._archive.query(
                {
                    "fromBlock": from_block,
                    "toBlock": query.stop_block,
                    "fields": {
                        "block": {
                            "number": True,
                            "hash": True,
                            "parentHash": True,
                            "size": True,
                            "timestamp": True,
                            "gasLimit": True,
                            "gasUsed": True,
                            "baseFeePerGas": True,
                            "difficulty": True,
                            "totalDifficulty": True,
                        },
                    },
                    "includeAllBlocks": True,
                    "transactions": [{}],
                }
            )

            for block in data:
                header_data = map_header(block["header"], block["transactions"])
                yield self.provider.network.ecosystem.decode_block(header_data)

            last_block = data[-1]
            if last_block["header"]["number"] == query.stop_block:
                break

            from_block = last_block["header"]["number"] + 1

    @perform_query.register
    def perform_account_transaction_query(
        self, query: AccountTransactionQuery
    ) -> Iterator[ReceiptAPI]:
        from_block = 0
        while True:
            data = self._archive.query(
                {
                    "fromBlock": from_block,
                    "fields": {
                        "transaction": {
                            "from": True,
                            "to": True,
                            "hash": True,
                            "status": True,
                            "chainId": True,
                            "contractAddress": True,
                            "cumulativeGasUsed": True,
                            "effectiveGasPrice": True,
                            "gas": True,
                            "gasPrice": True,
                            "gasUsed": True,
                            "input": True,
                            "maxFeePerGas": True,
                            "maxPriorityFeePerGas": True,
                            "nonce": True,
                            "v": True,
                            "r": True,
                            "s": True,
                            "transactionIndex": True,
                            "type": True,
                            "value": True,
                            "yParity": True,
                        },
                        "log": {
                            "address": True,
                            "data": True,
                            "logIndex": True,
                            "topics": True,
                            "transactionHash": True,
                            "transactionIndex": True,
                        },
                    },
                    "transactions": [
                        {
                            "from": [query.account.lower()],
                            "logs": True,
                            "firstNonce": query.start_nonce,
                            "lastNonce": query.stop_nonce,
                        }
                    ],
                }
            )

            for block in data:
                for tx in block["transactions"]:
                    assert tx["nonce"] >= query.start_nonce
                    assert tx["nonce"] <= query.stop_nonce

                    block_number = block["header"]["number"]
                    block_hash = HexBytes(block["header"]["hash"])

                    logs = []
                    for log in block["logs"]:
                        if log["transactionIndex"] == tx["transactionIndex"]:
                            log_data = map_log(log, block_number, block_hash)
                            logs.append(log_data)

                    receipt_data = map_receipt(tx, block_number, block_hash, logs)
                    receipt = self.provider.network.ecosystem.decode_receipt(receipt_data)
                    yield receipt

                    if tx["nonce"] == query.stop_nonce:
                        return

            last_block = data[-1]
            from_block = last_block["header"]["number"] + 1

    @perform_query.register
    def perform_contract_creation_query(self, query: ContractCreationQuery) -> Iterator[ReceiptAPI]:
        from_block = query.start_block
        while True:
            data = self._archive.query(
                {
                    "fromBlock": from_block,
                    "toBlock": query.stop_block,
                    "fields": {
                        "transaction": {
                            "from": True,
                            "to": True,
                            "hash": True,
                            "status": True,
                            "chainId": True,
                            "contractAddress": True,
                            "cumulativeGasUsed": True,
                            "effectiveGasPrice": True,
                            "gas": True,
                            "gasPrice": True,
                            "gasUsed": True,
                            "input": True,
                            "maxFeePerGas": True,
                            "maxPriorityFeePerGas": True,
                            "nonce": True,
                            "v": True,
                            "r": True,
                            "s": True,
                            "transactionIndex": True,
                            "type": True,
                            "value": True,
                            "yParity": True,
                        },
                        "log": {
                            "address": True,
                            "data": True,
                            "logIndex": True,
                            "topics": True,
                            "transactionHash": True,
                            "transactionIndex": True,
                        },
                        "trace": {
                            "transactionIndex": True,
                            "createResultAddress": True,
                        },
                    },
                    "traces": [{"type": ["create"], "transaction": True, "transactionLogs": True}],
                }
            )

            for block in data:
                for trace in block["traces"]:
                    if "result" in trace:
                        if trace["result"]["address"] == query.contract:
                            block_number = block["header"]["number"]
                            block_hash = HexBytes(block["header"]["hash"])
                            tx = next(
                                (
                                    tx
                                    for tx in block["transactions"]
                                    if tx["transactionIndex"] == trace["transactionIndex"]
                                )
                            )

                            logs = []
                            for log in block["logs"]:
                                if log["transactionIndex"] == tx["transactionIndex"]:
                                    log_data = map_log(log, block_number, block_hash)
                                    logs.append(log_data)

                            receipt_data = map_receipt(tx, block_number, block_hash, logs)
                            receipt = self.provider.network.ecosystem.decode_receipt(receipt_data)
                            yield receipt
                            return

            last_block = data[-1]
            if last_block["header"]["number"] == query.stop_block:
                break

            from_block = last_block["header"]["number"] + 1

    @perform_query.register
    def perform_contract_event_query(self, query: ContractEventQuery) -> Iterator[ContractLog]:
        from_block = query.start_block

        if isinstance(query.contract, list):
            address = [address.lower() for address in query.contract]
        else:
            address = [query.contract.lower()]

        while True:
            data = self._archive.query(
                {
                    "fromBlock": from_block,
                    "toBlock": query.stop_block,
                    "fields": {
                        "log": {
                            "address": True,
                            "data": True,
                            "logIndex": True,
                            "topics": True,
                            "transactionHash": True,
                            "transactionIndex": True,
                        },
                    },
                    "logs": [{"address": address}],
                }
            )

            for block in data:
                block_number = block["header"]["number"]
                block_hash = HexBytes(block["header"]["hash"])

                logs = []
                for log in block["logs"]:
                    log_data = map_log(log, block_number, block_hash)
                    logs.append(log_data)

                yield from self.provider.network.ecosystem.decode_logs(logs, query.event)

            last_block = data[-1]
            if last_block["header"]["number"] == query.stop_block:
                break

            from_block = last_block["header"]["number"] + 1
