"""Raytracing DAG for cumulative radiation."""

from pollination_dsl.dag import Inputs, DAG, task
from dataclasses import dataclass

from pollination.honeybee_radiance.grid import SplitGrid, MergeFiles
from pollination.honeybee_radiance.contrib import DaylightContribution
from pollination.honeybee_radiance.coefficient import DaylightCoefficient
from pollination.honeybee_radiance.post_process import CumulativeRadiation


@dataclass
class CumulativeRadiationRayTracing(DAG):
    # inputs

    wea = Inputs.file(
        description='Wea file.', extensions=['wea']
    )

    timestep = Inputs.int(
        description='Input wea timestep. This value will be used to compute '
        'cumulative radiation results.', default=1,
        spec={'type': 'integer', 'minimum': 1, 'maximum': 60}
    )

    sensor_count = Inputs.int(
        default=200,
        description='The maximum number of grid points per parallel execution',
        spec={'type': 'integer', 'minimum': 1}
    )

    radiance_parameters = Inputs.str(
        description='The radiance parameters for ray tracing',
        default='-ab 2 -ad 5000 -lw 2e-05'
    )

    octree_file = Inputs.file(
        description='A Radiance octree file without suns or sky.',
        extensions=['oct']
    )

    grid_name = Inputs.str(
        description='Sensor grid file name. This is useful to rename the final result '
        'file to {grid_name}.res'
    )

    sensor_grid = Inputs.file(
        description='Sensor grid file.',
        extensions=['pts']
    )

    sky_matrix = Inputs.file(
        description='Path to skymtx file.'
    )

    sky_dome = Inputs.file(
        description='Path to sky dome file.'
    )

    bsdfs = Inputs.folder(
        description='Folder containing any BSDF files needed for ray tracing.',
        optional=True
    )

    @task(template=SplitGrid)
    def split_grid(self, sensor_count=sensor_count, input_grid=sensor_grid):
        return [
            {'from': SplitGrid()._outputs.grids_list},
            {'from': SplitGrid()._outputs.output_folder, 'to': '00_sub_grids'}
        ]

    @task(
        template=DaylightCoefficient, needs=[split_grid],
        loop=split_grid._outputs.grids_list, sub_folder='01_radiation',
        sub_paths={'sensor_grid': '{{item.path}}'}
    )
    def total_sky(
        self,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -c 1',
        sensor_count='{{item.count}}',
        sky_matrix=sky_matrix, sky_dome=sky_dome,
        sensor_grid=split_grid._outputs.output_folder,
        conversion='0.265 0.670 0.065',  # divide by 179
        scene_file=octree_file,
        output_format='a',
        bsdf_folder=bsdfs
    ):
        return [
            {
                'from': DaylightCoefficient()._outputs.result_file,
                'to': '{{item.name}}.res'
            }
        ]

    @task(
        template=MergeFiles, needs=[total_sky]
    )
    def merge_results(
            self, name=grid_name, extension='.res', folder='01_radiation'):
        return [
            {
                'from': MergeFiles()._outputs.result_file,
                'to': '../../results/average_irradiance/{{self.name}}.res'
            }
        ]

    @task(
        template=CumulativeRadiation, needs=[merge_results]
    )
    def accumulate_results(
        self, name=grid_name, average_irradiance=merge_results._outputs.result_file,
        wea=wea, timestep=timestep
    ):
        return [
            {
                'from': CumulativeRadiation()._outputs.radiation,
                'to': '../../results/cumulative_radiation/{{self.name}}.res'
            }
        ]
