from ape import plugins

from ape_subsquid.query import SubsquidQueryEngine, get_network_height

__all__ = ["exceptions", "get_network_height"]


@plugins.register(plugins.QueryPlugin)
def query_engines():
    yield SubsquidQueryEngine
