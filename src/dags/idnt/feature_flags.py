class FeatureFlags:
    # Cookieless graph generation is on demand. Please note that there is no cookieless generation
    # support in OpenGraph right now.
    # TODO(leonid.vasilev): Remove all code blocks under this switch after some time if there no demand.
    enable_cookieless_graph_generation = False
