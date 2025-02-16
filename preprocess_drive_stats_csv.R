# Preprocess for Backblaze drive_stats csv files
# 2018-06-11 kennel.org
# 2025-02-16 Refined the script for 2024Q4 data

library("RMariaDB")
library("tictoc")
library("stringr")
library("purrr")
library("dplyr")
library("lubridate")
library("tidyverse")

get_script_directory <- function() {
  # Check for command line argument for --file= (Rscript execution)
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args, value = TRUE)
  if (length(file_arg) > 0) {
    # Extract the script path from the --file= argument
    script_path <- normalizePath(sub("^--file=", "", file_arg))
    # Return the directory containing the script
    return(dirname(script_path))
  }
  
  # Check if running in RStudio and get the active document's path
  if (requireNamespace("rstudioapi", quietly = TRUE) &&
      rstudioapi::isAvailable()) {
    script_path <- rstudioapi::getActiveDocumentContext()$path
    if (nzchar(script_path)) {
      # Return the directory containing the active script
      return(dirname(normalizePath(script_path)))
    }
  }
  
  # Return NA if the script directory could not be determined
  return(NA)
}

# Set working directory to the script's directory
script_directory <- get_script_directory()
print(script_directory)
setwd(script_directory)

outfile_path <- "outfile/"
infile_path <- "infile/"
tmp_path <- "tmp/"

#
# Read table template
#

drive_stats_columns <-
  colnames(read.csv(paste0(infile_path, "drive_stats_columns_template.csv")))

#
# Process invalid data
#

if (!file.exists(paste0(infile_path, "csv/2022-12-10_11.csv")) &&
    file.exists(paste0(infile_path, "csv/2022-12-10.csv")) &&
    file.exists(paste0(infile_path, "csv/2022-12-11.csv"))) {
  file_list <- c("2022-12-10.csv", "2022-12-11.csv")
  df <- map_dfr(file_list, ~ read.csv(paste0(infile_path, 'csv/', .x), stringsAsFactors = FALSE))
  df <- df %>%
    arrange(desc(failure), desc(smart_9_raw)) %>%
    distinct(date, serial_number, .keep_all = TRUE)
  
  output_file <- paste0(infile_path, "csv/2022-12-10_11.csv")
  write.csv(df, file = output_file, row.names = FALSE, quote = FALSE, na = "")
  
  target_folder <- paste0(infile_path, "skip_csv")
  if (!dir.exists(target_folder)) {
    dir.create(target_folder)
  }
  
  for (file_name in file_list) {
    source_file <- paste0(infile_path, "csv/", file_name)
    target_file <- paste0(target_folder, "/", file_name)
    file.copy(source_file, target_file)
    file.remove(source_file)
  }
}

#
# Remove empty data file
#

initial_dir <- paste0(infile_path, "csv/")

file_list <- list.files(
  initial_dir,
  recursive = FALSE,
  include.dirs = TRUE,
  pattern = "csv",
  full.names = TRUE
)

target_dir <- paste0(infile_path, "empty_csv/")

if (!dir.exists(target_dir)) {
  dir.create(target_dir, recursive = TRUE)
}

for (path in file_list) {
  if (file.info(path)$size < 4096) {
    print(path)
    target_file <- paste0(target_dir, basename(path))
    file.copy(path, target_file)
    file.remove(path)
  }
}

# Create Monthly csv file

initial_dir <- paste0(infile_path, "csv/")

file_list <- list.files(
  initial_dir,
  recursive = FALSE,
  include.dirs = FALSE,
  pattern = "csv",
  full.names = TRUE
)

save_dir <- paste0(infile_path, "csv_year_month/")

file_dates <- str_extract(basename(file_list), "^\\d{4}-\\d{2}")
unique_dates <- unique(file_dates)

if (!dir.exists(save_dir)) {
  dir.create(save_dir, recursive = TRUE)
}

for (date in unique_dates) {
  tic()
  gc()
  monthly_files <- file_list[file_dates == date]
  print(date)
  
  # Fixed: Corrected the syntax in the function passed to map_df
  x <- map_df(monthly_files, function(file) {
    df <- read_csv(file, col_types = cols())
    df <- df %>%
      mutate(
        source_file = basename(file),  # Add source_file column with filename
        modified = 0                   # Add modified column with constant 0
      )
    return(df)
  })
  
  colnames(x)[3] <- 'model_backblaze'
  
  if (nrow(x) > 0) {
    x <- x %>%
      mutate(
        # Remove leading and trailing spaces from model_backblaze column
        model_backblaze = str_trim(model_backblaze),
        # Extract the first word from model_backblaze (used for vendor or model extraction)
        tmp1 = str_extract(model_backblaze, "^[^ ]+"),
        # Remove the first word and trim the remaining text (used for model extraction)
        tmp2 = str_trim(str_replace(model_backblaze, tmp1, "")),
        # Set vendor based on model_backblaze content
        vendor = case_when(
          str_detect(model_backblaze, "^ST") ~ "Seagate",
          str_detect(model_backblaze, "^CT") ~ "Crucial",
          str_detect(model_backblaze, "^MT") ~ "Micron",
          str_detect(model_backblaze, "^SSDSC") ~ "Intel",
          str_detect(model_backblaze, "^DELLBOSS") ~ "Dell",
          TRUE ~ tmp1
        ),
        # Set model based on model_backblaze content
        model = case_when(
          str_detect(model_backblaze, "^ST") ~ tmp1,
          str_detect(model_backblaze, "^CT") ~ tmp1,
          str_detect(model_backblaze, "^MT") ~ tmp1,
          str_detect(model_backblaze, "^SSDSC") ~ tmp1,
          str_detect(model_backblaze, "^DELLBOSS") ~ tmp2,
          str_detect(model_backblaze, "ZA") ~ word(tmp2, -1),
          TRUE ~ tmp2
        )
      ) %>%
      # Replace NA in model column with "unknown"
      mutate(model = ifelse(is.na(model), "unknown", model)) %>%
      # Remove temporary columns
      select(-tmp1, -tmp2)
    
    missing_columns <- setdiff(drive_stats_columns, names(x))
    for (col in missing_columns) {
      x[[col]] <- NA
    }
    
    # Order columns to match drive_stats_columns
    x <- x[drive_stats_columns]
    
    write_csv(
      x,
      paste0(save_dir, "drive_stats_", date, ".csv"),
      na = "",
      col_names = FALSE
    )
  }
  toc()
}
