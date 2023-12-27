from typing import Literal, Optional, TypedDict, Union

from requests import Session

TraceType = Union[Literal["create"], Literal["call"], Literal["reward"], Literal["suicide"]]


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


TxFieldSelection = TypedDict(
    "TxFieldSelection",
    {
        "transactionIndex": bool,
        "hash": bool,
        "nonce": bool,
        "from": bool,
        "to": bool,
        "input": bool,
        "value": bool,
        "gas": bool,
        "gasPrice": bool,
        "maxFeePerGas": bool,
        "maxPriorityFeePerGas": bool,
        "v": bool,
        "r": bool,
        "s": bool,
        "yParity": bool,
        "chainId": bool,
        "contractAddress": bool,
        "gasUsed": bool,
        "cumulativeGasUsed": bool,
        "effectiveGasPrice": bool,
        "type": bool,
        "status": bool,
    },
    total=False,
)


class LogFieldSelection(TypedDict, total=False):
    logIndex: bool
    transactionIndex: bool
    transactionHash: bool
    address: bool
    data: bool
    topics: bool


class TraceFieldSelection(TypedDict, total=False):
    traceAddress: bool
    subtraces: bool
    transactionIndex: bool
    type: bool
    error: bool
    revertReason: bool
    createFrom: bool
    createValue: bool
    createGas: bool
    createInit: bool
    createResultGasUsed: bool
    createResultCode: bool
    createResultAddress: bool
    callFrom: bool
    callTo: bool
    callValue: bool
    callGas: bool
    callInput: bool
    callSighash: bool
    callType: bool
    callResultGasUsed: bool
    callResultOutput: bool
    suicideAddress: bool
    suicideRefundAddress: bool
    suicideBalance: bool
    rewardAuthor: bool
    rewardValue: bool
    rewardType: bool


class FieldSelection(TypedDict, total=False):
    block: BlockFieldSelection
    transaction: TxFieldSelection
    log: LogFieldSelection
    trace: TraceFieldSelection


TxRequest = TypedDict(
    "TxRequest",
    {
        "from": list[str],
        "to": list[str],
        "sighash": list[str],
        "firstNonce": int,
        "lastNonce": int,
        "logs": bool,
        "traces": bool,
        "stateDiffs": bool,
    },
    total=False,
)


class LogRequest(TypedDict, total=False):
    address: list[str]
    topic0: list[str]
    topic1: list[str]
    topic2: list[str]
    topic3: list[str]
    transaction: bool
    transactionTraces: bool
    transactionLogs: bool


class TraceRequest(TypedDict, total=False):
    type: list[TraceType]
    transaction: bool
    transactionLogs: bool


class Query(TypedDict, total=False):
    fromBlock: int
    toBlock: int
    includeAllBlocks: bool
    fields: FieldSelection
    transactions: list[TxRequest]
    logs: list[LogRequest]
    traces: list[TraceRequest]


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
    address: str
    transactionIndex: int
    transactionHash: str
    logIndex: int
    data: str
    topics: list[str]


Transaction = TypedDict(
    "Transaction",
    {
        "from": str,
        "to": Optional[str],
        "hash": str,
        "gas": str,
        "gasPrice": str,
        "maxFeePerGas": Optional[str],
        "maxPriorityFeePerGas": Optional[str],
        "input": str,
        "nonce": int,
        "transactionIndex": int,
        "value": str,
        "yParity": Optional[int],
        "chainId": Optional[int],
        "gasUsed": str,
        "cumulativeGasUsed": str,
        "effectiveGasPrice": str,
        "contractAddress": Optional[str],
        "type": int,
        "status": int,
        "v": str,
        "r": str,
        "s": str,
    },
)


class TraceCreateActionResult(TypedDict):
    gasUsed: int
    code: str
    address: str


class Trace(TypedDict, total=False):
    type: TraceType
    transactionIndex: int
    result: TraceCreateActionResult


class Block(TypedDict, total=False):
    header: BlockHeader
    logs: list[Log]
    transactions: list[Transaction]
    traces: list[Trace]


class Archive:
    _session = Session()

    def get_worker(self, start_block: int) -> str:
        url = f"https://v2.archive.subsquid.io/network/ethereum-mainnet/{start_block}/worker"
        response = self._session.get(url)
        response.raise_for_status()
        return response.text

    def query(self, query: Query) -> list[Block]:
        worker_url = self.get_worker(query["fromBlock"])
        response = self._session.post(worker_url, json=query)
        response.raise_for_status()
        return response.json()
