from ape.managers.networks import NetworkManager


def get_network(network_manager: NetworkManager) -> str:
    ecosystem_name = network_manager.ecosystem.name
    network_name = network_manager.network.name

    if ecosystem_name == "bsc":
        ecosystem_name = "binance"
    elif ecosystem_name == "arbitrum":
        if network_name == "mainnet":
            network_name = "one"

    return f"{ecosystem_name}-{network_name}"
