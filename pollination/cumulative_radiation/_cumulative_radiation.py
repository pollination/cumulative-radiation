from pollination_dsl.dag import Inputs, DAG, task
from dataclasses import dataclass
from pollination.honeybee_radiance.post_process import CumulativeRadiation


@dataclass
class CumulativeRadiationPostprocess(DAG):
    """Prepare folder for cumulative radiation."""

    # inputs
    grid_name = Inputs.str(
        description='Sensor grid file name. This is useful to rename the final result '
        'file to {grid_name}.ill'
    )

    average_irradiance = Inputs.file(
        description='A single-column matrix of average irradiance values in '
        'ASCII format', path='avg_irr.mtx'
    )

    wea = Inputs.file(
        description='The .wea file that was used in the simulation. This will be '
        'used to determine the duration of the analysis.', path='weather.wea'
    )

    timestep = Inputs.int(
        description='The timestep of the Wea file, which is used to to compute '
        'cumulative radiation over the time period of the Wea.', default=1
    )

    @task(template=CumulativeRadiation)
    def accumulate_results(
        self, name=grid_name, average_irradiance=average_irradiance,
        wea=wea, timestep=timestep
    ):
        return [
            {
                'from': CumulativeRadiation()._outputs.radiation,
                'to': 'results/cumulative_radiation/{{self.name}}.res'
            }
        ]
