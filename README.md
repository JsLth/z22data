Data repository for the [z22 R package](https://github.com/jslth/z22). Similar
to the [z11data](https://github.com/stefanjuenger/z11data) repository, it
contains the gridded
[Census 2022](https://www.zensus2022.de/DE/Ergebnisse-des-Zensus/gitterzellen.html)
data packed into smaller, more digestible parquet chunks. 

If you wish to work with the census data offline, you can download this
repository and point the z22 package to it by setting
`options(z22.data_repo = "path/to/z22data")`.

The `data-raw` directory contains the steps to reproduce the data sanitation
strategy. The `data_100m` directory contains the 100m grid data and the
`data_1km` directory contains the 1km grid data.

Generally, the German Census 2022 data are available under a [Data licence
Germany – attribution – version 2.0](https://www.govdata.de/dl-de/by-2-0) and
can be manipulated and openly shared. **Yet, as part of this package, use them
at your own risk and do not take the results of the functions for granted.**
