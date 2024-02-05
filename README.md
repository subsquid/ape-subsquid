# Quick Start

This plugin allows to use [subsquid data lake](https://docs.subsquid.io/subsquid-network/overview/) as a backend for ApeWorX query engine. A list of supported networks can be found [here](https://docs.subsquid.io/subsquid-network/reference/evm-networks/).

## Dependencies

- [python3](https://www.python.org/downloads) version 3.8 up to 3.11.

## Installation

### via `pip`

You can install the latest release via [`pip`](https://pypi.org/project/pip/):

```bash
pip install ape-subsquid
```

### via `setuptools`

You can clone the repository and use [`setuptools`](https://github.com/pypa/setuptools) for the most up-to-date version:

```bash
git clone https://github.com/subsquid/ape-subsquid.git
cd ape-subsquid
python3 setup.py install
```

## Quick Usage

If we want to be sure that subsquid engine is used then we have to specify it explicitly. Otherwise other query engines might be preferred depending on their estimated time.

The following queries can be executed via ApeWorX interactive console. Use `ape console --network ethereum:mainnet:geth` to run a console session.

```python
# BlockQuery
chain.blocks.query("*", start_block=18_000_000, stop_block=18_000_010, engine_to_use='subsquid')

# ContractEventQuery
contract = Contract('0xdac17f958d2ee523a2206206994597c13d831ec7', abi='<USDT_ABI>')
contract.Transfer.query('*', start_block=18_000_000, stop_block=18_000_100, engine_to_use='subsquid')
```

Supported queries are: `BlockQuery`, `AccountTransactionQuery`, `ContractCreationQuery`, `ContractEventQuery`.
More info about querying data can be found in the [corresponding guide](https://docs.apeworx.io/ape/stable/userguides/data.html).

## Development

Please see the [contributing guide](CONTRIBUTING.md) to learn more how to contribute to this project.
Comments, questions, criticisms and pull requests are welcomed.
