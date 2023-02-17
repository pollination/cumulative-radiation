from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass
from pollination.honeybee_radiance.grid import MergeFolderData
from pollination.honeybee_radiance.coefficient import DaylightCoefficient
from pollination.honeybee_radiance.post_process import CumulativeRadiation


# input/output alias
from pollination.alias.inputs.model import hbjson_model_grid_input
from pollination.alias.inputs.wea import wea_input
from pollination.alias.inputs.north import north_input
from pollination.alias.inputs.grid import grid_filter_input, \
    min_sensor_count_input, cpu_count
from pollination.alias.inputs.radiancepar import rad_par_annual_input
from pollination.alias.outputs.daylight import average_irradiance_results, \
    cumulative_radiation_results

from ._prepare_folder import CumulativeRadiationPrepareFolder
from ._cumulative_radiation import CumulativeRadiationPostprocess


@dataclass
class CumulativeRadiationEntryPoint(DAG):
    """Cumulative Radiation entry point."""

    # inputs
    timestep = Inputs.int(
        description='Input wea timestep. This value will be used to compute '
        'cumulative radiation results.', default=1,
        spec={'type': 'integer', 'minimum': 1, 'maximum': 60}
    )

    north = Inputs.float(
        default=0,
        description='A number for rotation from north.',
        spec={'type': 'number', 'minimum': -360, 'maximum': 360},
        alias=north_input
    )

    cpu_count = Inputs.int(
        default=50,
        description='The maximum number of CPUs for parallel execution. This will be '
        'used to determine the number of sensors run by each worker.',
        spec={'type': 'integer', 'minimum': 1},
        alias=cpu_count
    )

    min_sensor_count = Inputs.int(
        description='The minimum number of sensors in each sensor grid after '
        'redistributing the sensors based on cpu_count. This value takes '
        'precedence over the cpu_count and can be used to ensure that '
        'the parallelization does not result in generating unnecessarily small '
        'sensor grids. The default value is set to 1, which means that the '
        'cpu_count is always respected.', default=500,
        spec={'type': 'integer', 'minimum': 1},
        alias=min_sensor_count_input
    )

    radiance_parameters = Inputs.str(
        description='Radiance parameters for ray tracing.',
        default='-ab 2 -ad 5000 -lw 2e-05',
        alias=rad_par_annual_input
    )

    grid_filter = Inputs.str(
        description='Text for a grid identifier or a pattern to filter the sensor grids '
        'of the model that are simulated. For instance, first_floor_* will simulate '
        'only the sensor grids that have an identifier that starts with '
        'first_floor_. By default, all grids in the model will be simulated.',
        default='*',
        alias=grid_filter_input
    )

    sky_density = Inputs.int(
        default=1,
        description='The density of generated sky. This input corresponds to gendaymtx '
        '-m option. -m 1 generates 146 patch starting with 0 for the ground and '
        'continuing to 145 for the zenith. Increasing the -m parameter yields a higher '
        'resolution sky using the Reinhart patch subdivision. For example, setting -m 4 '
        'yields a sky with 2305 patches plus one patch for the ground.',
        spec={'type': 'integer', 'minimum': 1}
    )

    model = Inputs.file(
        description='A Honeybee model in HBJSON file format.',
        extensions=['json', 'hbjson', 'pkl', 'hbpkl', 'zip'],
        alias=hbjson_model_grid_input
    )

    wea = Inputs.file(
        description='Wea file.',
        extensions=['wea'],
        alias=wea_input
    )

    @task(template=CumulativeRadiationPrepareFolder)
    def prepare_folder_cumulative_radiation(
        self, timestep=timestep, north=north,
        cpu_count=cpu_count, min_sensor_count=min_sensor_count,
        grid_filter=grid_filter, sky_density=sky_density, model=model, wea=wea
    ):
        return [
            {
                'from': CumulativeRadiationPrepareFolder()._outputs.model_folder,
                'to': 'model'
            },
            {
                'from': CumulativeRadiationPrepareFolder()._outputs.resources,
                'to': 'resources'
            },
            {
                'from': CumulativeRadiationPrepareFolder()._outputs.results,
                'to': 'results'
            },
            {
                'from': CumulativeRadiationPrepareFolder()._outputs.initial_results,
                'to': 'initial_results'
            },
            {
                'from': CumulativeRadiationPrepareFolder()._outputs.sensor_grids
            },
            {
                'from': CumulativeRadiationPrepareFolder()._outputs.grids_info
            }
        ]

    @task(
        template=DaylightCoefficient,
        needs=[prepare_folder_cumulative_radiation],
        loop=prepare_folder_cumulative_radiation._outputs.sensor_grids,
        sub_folder='initial_results/{{item.full_id}}',  # subfolder for each grid
        sub_paths={
            'sky_dome': 'sky.dome',
            'sky_matrix': 'sky.mtx',
            'scene_file': 'scene.oct',
            'sensor_grid': 'grid/{{item.full_id}}.pts',
            'bsdf_folder': 'bsdf'
            }
    )
    def sky_radiation_raytracing(
        self,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -c 1',
        sensor_count='{{item.count}}',
        sky_dome=prepare_folder_cumulative_radiation._outputs.resources,
        sky_matrix=prepare_folder_cumulative_radiation._outputs.resources,
        scene_file=prepare_folder_cumulative_radiation._outputs.resources,
        sensor_grid=prepare_folder_cumulative_radiation._outputs.resources,
        conversion='0.265 0.670 0.065',
        output_format='a',
        bsdf_folder=prepare_folder_cumulative_radiation._outputs.model_folder
    ):
        return [
            {
                'from': DaylightCoefficient()._outputs.result_file,
                'to': '../{{item.name}}.res'
            }
        ]

    @task(
        template=MergeFolderData,
        needs=[sky_radiation_raytracing]
    )
    def restructure_results(self, input_folder='initial_results', extension='res'):
        return [
            {
                'from': MergeFolderData()._outputs.output_folder,
                'to': 'results/average_irradiance'
            }
        ]

    @task(
        template=CumulativeRadiationPostprocess,
        needs=[prepare_folder_cumulative_radiation, restructure_results],
        loop=prepare_folder_cumulative_radiation._outputs.grids_info,
        sub_paths={'average_irradiance': '{{item.full_id}}.res'}
    )
    def cumulative_radiation_postprocess(
        self, grid_name='{{item.full_id}}',
        average_irradiance=restructure_results._outputs.output_folder,
        wea=wea, timestep=timestep
    ):
        pass

    average_irradiance = Outputs.folder(
        source='results/average_irradiance', description='The average irradiance in '
        'W/m2 for each sensor over the Wea time period.',
        alias=average_irradiance_results
    )

    cumulative_radiation = Outputs.folder(
        source='results/cumulative_radiation', description='The cumulative radiation '
        'in kWh/m2 over the Wea time period.', alias=cumulative_radiation_results
    )
