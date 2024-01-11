from time import sleep
from typing import Literal, Optional, TypedDict, Union

from ape.logging import logger
from requests import Response, Session
from requests.exceptions import HTTPError

from ape_subsquid.exceptions import ApeSubsquidError, DataIsNotAvailable, NotReadyToServeError

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
    createResultAddress: list[str]
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
    _retry_schedule = [5, 10, 20, 30, 60]

    def get_worker(self, network: str, start_block: int) -> str:
        url = f"https://v2.archive.subsquid.io/network/{network}/{start_block}/worker"
        response = self._request("GET", url)
        return response.text

    def query(self, network: str, query: Query) -> list[Block]:
        worker_url = self.get_worker(network, query["fromBlock"])
        response = self._request("POST", worker_url, json=query)
        return response.json()

    def _request(self, *args, **kwargs) -> Response:
        retries = 0
        while True:
            response = self._session.request(*args, **kwargs)
            try:
                response.raise_for_status()
            except HTTPError as e:
                if self._is_retryable_error(e) and retries < len(self._retry_schedule):
                    pause = self._retry_schedule[retries]
                    retries += 1
                    logger.warning(f"archive request failed, will retry in {pause} secs")
                    sleep(pause)
                else:
                    self._raise_error(e)
            else:
                return response

    def _is_retryable_error(self, error: HTTPError) -> bool:
        assert error.response
        return error.response.status_code == 503

    def _raise_error(self, error: HTTPError) -> ApeSubsquidError:
        assert error.response
        text = error.response.text
        if "not ready to serve block" in text:
            raise NotReadyToServeError(text)
        elif "is not available" in text:
            raise DataIsNotAvailable(text)
        else:
            raise ApeSubsquidError(text)
