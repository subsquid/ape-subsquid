from typing import NotRequired, Optional, TypedDict

from requests import Session


class BlockFieldSelection(TypedDict, total=False):
    number: bool
    hash: bool
    parentHash: bool
    timestamp: bool
    transactionsRoot: bool
    receiptsRoot: bool
    stateRoot: bool
    logsBloom: bool
    sha3Uncles: bool
    extraData: bool
    miner: bool
    nonce: bool
    mixHash: bool
    size: bool
    gasLimit: bool
    gasUsed: bool
    difficulty: bool
    totalDifficulty: bool
    baseFeePerGas: bool


class FieldSelection(TypedDict, total=False):
    block: BlockFieldSelection
    # transaction: TxFieldSelection
    # log: LogFieldSelection
    # trace: TraceFieldSelection


class Query(TypedDict):
    fromBlock: int
    toBlock: NotRequired[int]
    includeAllBlocks: NotRequired[bool]
    fields: NotRequired[FieldSelection]
    transactions: NotRequired[list[dict]]
    # logs: NotRequired[list[LogRequest]]
    # traces: NotRequired[list[TraceRequest]]


class BlockHeader(TypedDict):
    number: int
    hash: str
    parentHash: str
    size: int
    sha3Uncles: str
    miner: str
    stateRoot: str
    transactionsRoot: str
    receiptsRoot: str
    logsBloom: str
    difficulty: str
    totalDifficulty: str
    gasLimit: str
    gasUsed: str
    timestamp: float
    extraData: str
    mixHash: str
    nonce: str
    baseFeePerGas: Optional[str]


class Log(TypedDict):
    pass


class Transaction(TypedDict):
    pass


class Trace(TypedDict):
    pass


class Block(TypedDict):
    header: BlockHeader
    logs: NotRequired[list[Log]]
    transactions: NotRequired[list[Transaction]]
    traces: NotRequired[list[Trace]]


class Archive:
    _session = Session()

    def get_worker(self, start_block: int) -> str:
        url = f"https://v2.archive.subsquid.io/network/ethereum-mainnet/{start_block}/worker"
        response = self._session.get(url)
        return response.text

    def query(self, query: Query) -> list[Block]:
        worker_url = self.get_worker(query["fromBlock"])
        response = self._session.post(worker_url, json=query)
        return response.json()
