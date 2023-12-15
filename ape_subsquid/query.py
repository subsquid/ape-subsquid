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


class SubsquidQueryEngine(QueryAPI):
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
    def perform_block_query(self, query: BlockQuery):
        return None

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
