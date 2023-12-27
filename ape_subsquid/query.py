from typing import Iterator, Optional, Type, TypeVar, cast

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

from ape_subsquid.archive import (
    Archive,
    Block,
    BlockFieldSelection,
    BlockHeader,
    Log,
    LogFieldSelection,
    Query,
    Transaction,
    TxFieldSelection,
)

T = TypeVar("T")


def all_fields(cls: Type[T]) -> T:
    fields = cls.__annotations__
    for field in fields:
        fields[field] = True
    return cast(T, fields)


def hex_to_int(value: str):
    return int(value, 16)


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


def archive_ingest(archive: Archive, query: Query) -> Iterator[list[Block]]:
    while True:
        data = archive.query(query)
        yield data

        last_block = data[-1]
        if "toBlock" in query:
            if last_block["header"]["number"] == query["toBlock"]:
                break

        query["fromBlock"] = last_block["header"]["number"] + 1


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
        q: Query = {
            "fromBlock": query.start_block,
            "toBlock": query.stop_block,
            "fields": {"block": all_fields(BlockFieldSelection)},
            "includeAllBlocks": True,
            "transactions": [{}],
        }

        for data in archive_ingest(self._archive, q):
            for block in data:
                header_data = map_header(block["header"], block["transactions"])
                yield self.provider.network.ecosystem.decode_block(header_data)

    @perform_query.register
    def perform_account_transaction_query(
        self, query: AccountTransactionQuery
    ) -> Iterator[ReceiptAPI]:
        q: Query = {
            "fromBlock": 0,
            "fields": {
                "transaction": all_fields(TxFieldSelection),
                "log": all_fields(LogFieldSelection),
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

        for data in archive_ingest(self._archive, q):
            for block in data:
                for tx in block["transactions"]:
                    assert tx["nonce"] >= query.start_nonce
                    assert tx["nonce"] <= query.stop_nonce

                    block_number = block["header"]["number"]
                    block_hash = HexBytes(block["header"]["hash"])
                    logs = [
                        map_log(log, block_number, block_hash)
                        for log in block["logs"]
                        if log["transactionIndex"] == tx["transactionIndex"]
                    ]
                    receipt_data = map_receipt(tx, block_number, block_hash, logs)

                    yield self.provider.network.ecosystem.decode_receipt(receipt_data)

                    if tx["nonce"] == query.stop_nonce:
                        return

    @perform_query.register
    def perform_contract_creation_query(self, query: ContractCreationQuery) -> Iterator[ReceiptAPI]:
        q: Query = {
            "fromBlock": query.start_block,
            "toBlock": query.stop_block,
            "fields": {
                "transaction": all_fields(TxFieldSelection),
                "log": all_fields(LogFieldSelection),
                "trace": {
                    "transactionIndex": True,
                    "createResultAddress": True,
                },
            },
            "traces": [{"type": ["create"], "transaction": True, "transactionLogs": True}],
        }

        for data in archive_ingest(self._archive, q):
            for block in data:
                for trace in block["traces"]:
                    if "result" in trace:
                        if trace["result"]["address"] == query.contract:
                            block_number = block["header"]["number"]
                            block_hash = HexBytes(block["header"]["hash"])
                            tx = (
                                tx
                                for tx in block["transactions"]
                                if tx["transactionIndex"] == trace["transactionIndex"]
                            ).__next__()
                            logs = [
                                map_log(log, block_number, block_hash)
                                for log in block["logs"]
                                if log["transactionIndex"] == tx["transactionIndex"]
                            ]
                            receipt_data = map_receipt(tx, block_number, block_hash, logs)

                            yield self.provider.network.ecosystem.decode_receipt(receipt_data)
                            return

    @perform_query.register
    def perform_contract_event_query(self, query: ContractEventQuery) -> Iterator[ContractLog]:
        if isinstance(query.contract, list):
            address = [address.lower() for address in query.contract]
        else:
            address = [query.contract.lower()]

        q: Query = {
            "fromBlock": query.start_block,
            "toBlock": query.stop_block,
            "fields": {"log": all_fields(LogFieldSelection)},
            "logs": [{"address": address}],
        }

        for data in archive_ingest(self._archive, q):
            for block in data:
                block_number = block["header"]["number"]
                block_hash = HexBytes(block["header"]["hash"])
                logs = [map_log(log, block_number, block_hash) for log in block["logs"]]

                yield from self.provider.network.ecosystem.decode_logs(logs, query.event)
