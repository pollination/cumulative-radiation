from pollination_dsl.dag import Inputs, DAG, task, Outputs
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
        'cpu_count is always respected.', default=1,
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
        extensions=['json', 'hbjson'],
        alias=hbjson_model_grid_input
    )

    wea = Inputs.file(
        description='Wea file.',
        extensions=['wea'],
        alias=wea_input
    )

    @task(template=CreateRadianceFolderGrid)
    def create_rad_folder(self, input_model=model, grid_filter=grid_filter):
        """Translate the input model to a radiance folder."""
        return [
            {
                'from': CreateRadianceFolderGrid()._outputs.model_folder,
                'to': 'model'
            },
            {
                'from': CreateRadianceFolderGrid()._outputs.bsdf_folder,
                'to': 'model/bsdf'
            },
            {
                'from': CreateRadianceFolderGrid()._outputs.sensor_grids_file,
                'to': 'results/average_irradiance/grids_info.json'
            },
            {
                'from': CreateRadianceFolderGrid()._outputs.sensor_grids,
                'description': 'Sensor grids information.'
            }
        ]

    @task(template=Copy, needs=[create_rad_folder])
    def copy_grid_info(self, src=create_rad_folder._outputs.sensor_grids_file):
        return [
            {
                'from': Copy()._outputs.dst,
                'to': 'results/cumulative_radiation/grids_info.json'
            }
        ]

    @task(template=CreateOctree, needs=[create_rad_folder])
    def create_octree(
        self, model=create_rad_folder._outputs.model_folder
    ):
        """Create octree from radiance folder."""
        return [
            {
                'from': CreateOctree()._outputs.scene_file,
                'to': 'resources/scene.oct'
            }
        ]

    @task(
        template=SplitGridFolder, needs=[create_rad_folder],
        sub_paths={'input_folder': 'grid'}
    )
    def split_grid_folder(
        self, input_folder=create_rad_folder._outputs.model_folder,
        cpu_count=cpu_count, cpus_per_grid=1, min_sensor_count=min_sensor_count
    ):
        """Split sensor grid folder based on the number of CPUs"""
        return [
            {
                'from': SplitGridFolder()._outputs.output_folder,
                'to': 'resources/grid'
            },
            {
                'from': SplitGridFolder()._outputs.dist_info,
                'to': 'initial_results/_redist_info.json'
            },
            {
                'from': SplitGridFolder()._outputs.sensor_grids,
                'description': 'Sensor grids information.'
            }
        ]

    @task(template=CreateSkyDome)
    def create_sky_dome(self, sky_density=sky_density):
        """Create sky dome for daylight coefficient studies."""
        return [
            {'from': CreateSkyDome()._outputs.sky_dome, 'to': 'resources/sky.dome'}
        ]

    @task(template=CreateSkyMatrix)
    def create_sky(
        self, north=north, wea=wea, sky_type='total', output_type='solar',
        output_format='ASCII', sky_density=sky_density, cumulative='cumulative'
    ):
        return [
            {
                'from': CreateSkyMatrix()._outputs.sky_matrix,
                'to': 'resources/sky.mtx'
            }
        ]

    @task(
        template=DaylightCoefficient,
        needs=[create_sky_dome, create_octree, create_sky,
               create_rad_folder, split_grid_folder],
        loop=split_grid_folder._outputs.sensor_grids,
        sub_folder='initial_results/{{item.full_id}}',  # create a subfolder for each grid
        sub_paths={'sensor_grid': '{{item.full_id}}.pts'}
    )
    def sky_radiation_raytracing(
        self,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -c 1',
        sensor_count='{{item.count}}',
        sky_dome=create_sky_dome._outputs.sky_dome,
        sky_matrix=create_sky._outputs.sky_matrix,
        scene_file=create_octree._outputs.scene_file,
        sensor_grid=split_grid_folder._outputs.output_folder,
        conversion='0.265 0.670 0.065',
        output_format='a',
        bsdf_folder=create_rad_folder._outputs.bsdf_folder
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
        template=CumulativeRadiation,
        needs=[restructure_results, create_rad_folder],
        loop=create_rad_folder._outputs.sensor_grids,
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

    average_irradiance = Outputs.folder(
        source='results/average_irradiance', description='The average irradiance in '
        'W/m2 for each sensor over the Wea time period.',
        alias=average_irradiance_results
    )

    cumulative_radiation = Outputs.folder(
        source='results/cumulative_radiation', description='The cumulative radiation '
        'in kWh/m2 over the Wea time period.', alias=cumulative_radiation_results
    )
