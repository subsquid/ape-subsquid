from typing import Iterator, Optional, Type, TypeVar, cast

from ape import networks
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
from ape.logging import logger
from ape.types import ContractLog
from ape.utils import singledispatchmethod
from hexbytes import HexBytes

from ape_subsquid.exceptions import DataRangeIsNotAvailable
from ape_subsquid.gateway import (
    Block,
    BlockFieldSelection,
    LogFieldSelection,
    Query,
    SubsquidGateway,
    TxFieldSelection,
    gateway,
)
from ape_subsquid.mappings import map_header, map_log, map_receipt
from ape_subsquid.networks import get_network


class SubsquidQueryEngine(QueryAPI):
    _gateway = gateway

    @singledispatchmethod
    def estimate_query(self, query: QueryType) -> Optional[int]:  # type: ignore[override]
        return None

    @estimate_query.register
    def estimate_block_query(self, query: BlockQuery) -> int:
        return 100 + (query.stop_block - query.start_block) * 4

    @estimate_query.register
    def estimate_account_transaction_query(self, query: AccountTransactionQuery) -> int:
        # the entire network can be scanned in a worst-case scenario
        # so just high value is used (10 min)
        return 1000 * 60 * 10

    @estimate_query.register
    def estimate_contract_creation_query(self, query: ContractCreationQuery) -> int:
        return 100 + (query.stop_block - query.start_block) * 5

    @estimate_query.register
    def estimate_contract_event_query(self, query: ContractEventQuery) -> int:
        return 400 + (query.stop_block - query.start_block) * 4

    @singledispatchmethod
    def perform_query(self, query: QueryType) -> Iterator:  # type: ignore[override]
        raise QueryEngineError(
            f"{self.__class__.__name__} cannot handle {query.__class__.__name__} queries."
        )

    @perform_query.register
    def perform_block_query(self, query: BlockQuery) -> Iterator[BlockAPI]:
        network = get_network(self.network_manager)
        q: Query = {
            "fromBlock": query.start_block,
            "toBlock": query.stop_block,
            "fields": {"block": all_fields(BlockFieldSelection)},
            "includeAllBlocks": True,
            "transactions": [{}],
        }

        for data in gateway_ingest(self._gateway, network, q):
            for block in data:
                header_data = map_header(block["header"], block["transactions"])
                yield self.provider.network.ecosystem.decode_block(header_data)

    @perform_query.register
    def perform_account_transaction_query(
        self, query: AccountTransactionQuery
    ) -> Iterator[ReceiptAPI]:
        network = get_network(self.network_manager)
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

        for data in gateway_ingest(self._gateway, network, q):
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
        network = get_network(self.network_manager)
        contract = query.contract.lower()
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
            "traces": [
                {"createResultAddress": [contract], "transaction": True, "transactionLogs": True}
            ],
        }

        for data in gateway_ingest(self._gateway, network, q):
            for block in data:
                for trace in block["traces"]:
                    assert trace["result"]["address"] == contract

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
        network = get_network(self.network_manager)
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

        for data in gateway_ingest(self._gateway, network, q):
            for block in data:
                block_number = block["header"]["number"]
                block_hash = HexBytes(block["header"]["hash"])
                logs = [map_log(log, block_number, block_hash) for log in block["logs"]]

                yield from self.provider.network.ecosystem.decode_logs(logs, query.event)


T = TypeVar("T")


def all_fields(cls: Type[T]) -> T:
    fields = cls.__annotations__
    for field in fields:
        fields[field] = True
    return cast(T, fields)


def ensure_range_is_available(gateway: SubsquidGateway, network: str, query: Query):
    height = gateway.get_height(network)
    if query["toBlock"] > height:
        range = (query["fromBlock"], query["toBlock"])
        raise DataRangeIsNotAvailable(range, height)


def gateway_ingest(gateway: SubsquidGateway, network: str, query: Query) -> Iterator[list[Block]]:
    ensure_range_is_available(gateway, network, query)
    while True:
        data = gateway.query(network, query)
        yield data

        last_block = data[-1]["header"]["number"]
        logger.info(f"Done fetching the range ({query['fromBlock']}, {last_block})")
        if "toBlock" in query:
            if last_block == query["toBlock"]:
                break

        query["fromBlock"] = last_block + 1


def get_network_height() -> int:
    network = get_network(networks)
    height = gateway.get_height(network)
    return height
