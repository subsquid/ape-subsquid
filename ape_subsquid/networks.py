from ape.api.query import QueryAPI


def get_network(engine: QueryAPI) -> str:
    ecosystem_name = engine.network_manager.ecosystem.name
    network_name = engine.network_manager.network.name

    if ecosystem_name == "bsc":
        ecosystem_name = "binance"
    elif ecosystem_name == "arbitrum":
        if network_name == "mainnet":
            network_name = "one"

    return f"{ecosystem_name}-{network_name}"
