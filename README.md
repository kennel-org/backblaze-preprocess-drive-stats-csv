# Backblaze Drive Stats CSV Preprocessing

This repository contains an R script designed to preprocess Backblaze drive_stats CSV files. The script handles several tasks including merging specific files, cleaning invalid data, removing empty files, and generating consolidated monthly CSV files with a standardized schema.

## Data Source

The original CSV files can be downloaded from the [Backblaze Hard Drive Test Data](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data/) page. **After downloading, please extract the CSV files into the `infile/csv` directory.**

## Prerequisites

The script requires the following R packages:

- `RMariaDB`
- `tictoc`
- `stringr`
- `purrr`
- `dplyr`
- `lubridate`
- `tidyverse`

You can install these packages using the following command in R:

```r
install.packages(c("RMariaDB", "tictoc", "stringr", "purrr", "dplyr", "lubridate", "tidyverse"))
