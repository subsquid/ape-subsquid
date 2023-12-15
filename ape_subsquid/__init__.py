from ape import plugins

from ape_subsquid.query import SubsquidQueryEngine


@plugins.register(plugins.QueryPlugin)
def query_engines():
    yield SubsquidQueryEngine
