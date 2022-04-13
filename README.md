# Cumulative Radiation

Cumulative Radiation recipe for Pollination

This recipe calculates average irradiance (W/m2) and cumulative radiation (kWh/m2)
over the time period of a specified Wea.

## Limitations

This recipe uses Radiance's `gendaymtx` to generate the sky instead of directly
tracing the line of sight from sensors to the solar position.

```console
Gendaymtx takes a weather tape as input and produces a matrix of sky patch values
using the Perez all weather model. If there is a sun in the description, gendaymtx
will include its contribution in the four nearest sky patches, distributing energy
according to centroid proximity.
```

This means that the direct sun is diffused between several sky patches and so the
precise line between shadow and sun for each hour is blurred. This approximation
is fine for studies where the timestep-by-timestep irradiance values do not need
to be exact. For accurate modeling of direct irradiance on a timestep-by-timestep
basis, see the `annual_irradiance` recipe.
