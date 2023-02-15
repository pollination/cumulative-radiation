from pollination_dsl.dag import Inputs, GroupedDAG, task, Outputs
from dataclasses import dataclass
from pollination.honeybee_radiance.translate import CreateRadianceFolderGrid
from pollination.honeybee_radiance.grid import SplitGridFolder, MergeFolderData
from pollination.honeybee_radiance.octree import CreateOctree
from pollination.honeybee_radiance.sky import CreateSkyDome, CreateSkyMatrix
from pollination.honeybee_radiance.coefficient import DaylightCoefficient
from pollination.honeybee_radiance.post_process import CumulativeRadiation
from pollination.path.copy import Copy


# input/output alias
from pollination.alias.inputs.model import hbjson_model_grid_input
from pollination.alias.inputs.wea import wea_input
from pollination.alias.inputs.north import north_input
from pollination.alias.inputs.grid import grid_filter_input, \
    min_sensor_count_input, cpu_count
from pollination.alias.inputs.radiancepar import rad_par_annual_input
from pollination.alias.outputs.daylight import average_irradiance_results, \
    cumulative_radiation_results


@dataclass
class CumulativeRadiationPostprocess(GroupedDAG):
    """Post-process for cumulative radiation."""

    # inputs
    input_folder = Inputs.folder(
        description='Folder with initial results before redistributing the '
        'results to the original grids.'
    )

    grids_info = Inputs.list(
        description='Grids information from the original model.'
    )

    timestep = Inputs.file(
        description='Timestep file.'
    )

    wea = Inputs.file(
        description='Wea file.',
        extensions=['wea'],
        alias=wea_input
    )

    @task(template=MergeFolderData)
    def restructure_results(self, input_folder='initial_results', extension='res'):
        return [
            {
                'from': MergeFolderData()._outputs.output_folder,
                'to': 'results/average_irradiance'
            }
        ]

    @task(
        template=CumulativeRadiation,
        needs=[restructure_results],
        loop=grids_info,
        sub_paths={'average_irradiance': '{{item.name}}.res'}
    )
    def accumulate_results(
        self, average_irradiance=restructure_results._outputs.output_folder,
        wea=wea, timestep=timestep
    ):
        return [
            {
                'from': CumulativeRadiation()._outputs.radiation,
                'to': 'results/cumulative_radiation/{{item.name}}.res'
            }
        ]

    results = Outputs.folder(
        source='results', description='results folder.'
    )
