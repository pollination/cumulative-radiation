from pollination.cumulative_radiation.entry import CumulativeRadiationEntryPoint
from queenbee.recipe.dag import DAG


def test_cumulative_radiation():
    recipe = CumulativeRadiationEntryPoint().queenbee
    assert recipe.name == 'cumulative-radiation-entry-point'
    assert isinstance(recipe, DAG)
