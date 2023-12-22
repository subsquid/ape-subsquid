from typing import Iterator, Optional

from ape.api.query import (
    AccountTransactionQuery,
    BlockQuery,
    BlockTransactionQuery,
    ContractCreationQuery,
    ContractEventQuery,
    QueryAPI,
    QueryType,
)
from ape.exceptions import QueryEngineError
from ape.utils import singledispatchmethod
from ape_ethereum import ecosystem

from ape_subsquid.archive import Archive, Block


def map_block(block: Block) -> ecosystem.Block:
    return ecosystem.Block(
        number=block["header"]["number"],
        hash=block["header"]["hash"],
        parentHash=block["header"]["parentHash"],
        size=block["header"]["size"],
        timestamp=int(block["header"]["timestamp"]),
        num_transactions=len(block.get("transactions", [])),
        gasLimit=block["header"]["gasLimit"],
        gasUsed=block["header"]["gasUsed"],
        baseFeePerGas=block["header"]["baseFeePerGas"],
        difficulty=block["header"]["difficulty"],
        totalDifficulty=block["header"]["totalDifficulty"],
    )


class SubsquidQueryEngine(QueryAPI):
    _archive = Archive()

    @singledispatchmethod
    def estimate_query(self, query: QueryType) -> Optional[int]:
        return None

    @estimate_query.register
    def estimate_block_query(self, query: BlockQuery) -> int:
        router_ms = 400
        query_ms = 300
        return router_ms + query_ms

    @estimate_query.register
    def estimate_block_transaction_query(self, query: BlockTransactionQuery) -> int:
        router_ms = 400
        query_ms = 1500
        return router_ms + query_ms

    @estimate_query.register
    def estimate_account_transaction_query(self, query: AccountTransactionQuery) -> int:
        return 0

    @estimate_query.register
    def estimate_contract_creation_query(self, query: ContractCreationQuery) -> int:
        return 0

    @estimate_query.register
    def estimate_contract_event_query(self, query: ContractEventQuery) -> int:
        router_ms = 400
        query_ms = 400 + (1 + query.stop_block - query.start_block) * 1.4
        return router_ms + query_ms

    @singledispatchmethod
    def perform_query(self, query: QueryType) -> Iterator:
        raise QueryEngineError(
            f"{self.__class__.__name__} cannot handle {query.__class__.__name__} queries."
        )

    @perform_query.register
    def perform_block_query(self, query: BlockQuery) -> Iterator[ecosystem.Block]:
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
                yield map_block(block)

            last_block = data[-1]
            if last_block["header"]["number"] == query.stop_block:
                break

            from_block = last_block["header"]["number"] + 1

    @perform_query.register
    def perform_block_transaction_query(self, query: BlockTransactionQuery):
        return None

    @perform_query.register
    def perform_account_transaction_query(self, query: AccountTransactionQuery):
        return None

    @perform_query.register
    def perform_contract_creation_query(self, query: ContractCreationQuery):
        return None

    @perform_query.register
    def perform_contract_event_query(self, query: ContractEventQuery):
        return None
