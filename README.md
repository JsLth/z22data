Data repository for the [z22 R package](https://github.com/jslth/z22). Similar
to the [z11data](https://github.com/stefanjuenger/z11data) repository, it
contains the gridded
[Census 2022](https://www.zensus2022.de/EN/Census_results/_inhalt.html#)
data packed into smaller, more digestible parquet chunks. 

If you wish to work with the census data offline, you can download this
repository and point the z22 package to it by setting
`options(z22.data_repo = "path/to/z22data")`.

The repository contains both Census 2011 and 2022 data at various resolutions:

- `z11_data_1km`: Census 2011 at a 1 km resolution
- `z11_data_100m`: Census 2011 at a 100 m resolution
- `z22_data_10km`: Census 2022 at a 10 km resolution
- `z22_data_1km`: Census 2022 at a 1 km resolution
- `z22_data_100m`: Census 2022 at a 100m resolution

Note that not all datasets are available for each year and each resolution.
The file names follow a simple scheme: `{feature}_{category code}.parquet`
where `feature` is the translated name of the Census indicator and
`category code` is  the integer representation of the category. For features
with no categories, the category code is always 0. Otherwise, refer to the
official documentation of the `z22` to see what the category codes mean.
Generally, lower correspond to lower ordinal classes (e.g. lower codes
correspond to lower age bins).

INSPIRE grids can be found in the `grids` directory. They are taken from the
[GeoGitter dataset](https://gdz.bkg.bund.de/index.php/default/geographische-gitter-fur-deutschland-in-lambert-projektion-geogitter-inspire.html)
by the Federal Agency for Cartography and Geodesy (BKG). They can be useful
if a complete grid coverage is desired as the Census data only contain those
cells whose values are not missing.

Generally, the data BKG and German Census data are available under a [Data licence
Germany – attribution – version 2.0](https://www.govdata.de/dl-de/by-2-0) and
can be manipulated and openly shared. **Yet, as part of this repository, use them
at your own risk and do not take the correctness of the data files for granted.**
