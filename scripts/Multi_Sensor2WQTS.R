##
#     Multi-sensor2WQTS
#This R project proposes to create data management pipeline from multiple 
# make and models of data logging data sondes for collecting water quality data
# and save the processed data to a SQL Server database called WQTS (water quality time series).
# The initial project includes code and sample data from a HOBO water temperature and
# dissolved oxygen sensor/logger. 
# The first sensor/logger is the Hobo® DO/ Temperature data loggers (Model U26-001)
# The plan is to add additional sensors.
# The WQTS database was developed and provide by the NW Indian Fisheries Commission
# University of Washington Department of Statistics students and facility contributed 
# to this project.


library(tidyverse)
library(lubridate)
library(janitor)
library(rstudioapi)
library(stringr)

# Define sensor-specific column mappings
sensor_mappings <- list(
  "HOBO_U26-001_DO_Temp" = c('lnum', 'date_time', 'DO', 'temp_c', 'Attached', 'Stopped', 'End', 'filename')
)

# Embedded SQL Server schema (WQTS_Data)
sql_columns <- c(
  "TS_ID", "MONLOC_AB", "StartDate", "StartTime", "DataLoggerLineName",
  "Deployment", "ProjectIdentifier", "ActivityIdentifier", "ActivityMediaName",
  "CharacteristicName", "ResultMeasureValue", "ResultMeasureUnitCode",
  "DataType", "UseForCalc", "COMMENTS", "CreatedDateTime", "LastChangeDate"
)

# Prompt user to select sensor type
select_sensor_type <- function() {
  cat("Available sensor types:\n")
  sensor_names <- names(sensor_mappings)
  for (i in seq_along(sensor_names)) {
    cat(i, ": ", sensor_names[i], "\n")
  }
  choice <- as.integer(readline(prompt = "Select a sensor type by number: "))
  if (choice >= 1 && choice <= length(sensor_names)) {
    return(sensor_names[choice])
  } else {
    stop("Invalid selection.")
  }
}

# Cross-platform folder selection
select_data_folder <- function() {
  if (interactive() && rstudioapi::isAvailable()) {
    folder <- rstudioapi::selectDirectory()
    if (is.null(folder)) stop("No folder selected.")
    return(folder)
  } else {
    stop("Interactive folder selection requires RStudio.")
  }
}

# Extract serial number from column headers
extract_serial_number <- function(file_path) {
  header_lines <- readLines(file_path, n = 3)
  pattern <- "S/N:\\s*(\\d+)"
  matches <- str_match(header_lines[2], pattern)
  if (!is.na(matches[1, 2])) {
    return(matches[1, 2])
  } else {
    matches <- str_match(header_lines[3], pattern)
    if (!is.na(matches[1, 2])) {
      return(matches[1, 2])
    } else {
      warning("Serial number not found in header.")
      return(NA)
    }
  }
}

# Read and process files
read_plus <- function(flnm) {
  snum <- extract_serial_number(flnm)
  readr::read_csv(flnm, col_names = FALSE, skip = 2) %>%
    mutate(filename = basename(flnm), snum = snum)
}

# Main ingestion function
ingest_sensor_data <- function() {
  sensor_type <- select_sensor_type()
  col_names <- sensor_mappings[[sensor_type]]
  folder <- select_data_folder()
  
  files <- list.files(path = folder, pattern = "*.csv", full.names = TRUE)
  tbl_with_sources <- purrr::map_df(files, read_plus)
  names(tbl_with_sources)[1:length(col_names)] <- col_names
  
  # Extract site, media, deployment year and season from filename
  tbl_with_sources <- tbl_with_sources %>%
    separate(filename, into = c("site", "media", "deployment_full"), sep = "_", remove = FALSE) %>%
    mutate(
      deployment = str_remove(deployment_full, ".csv$"),
      deployment_year = str_extract(deployment, "^\\d{4}"),
      deployment_season = str_remove(deployment, "^\\d{4}"),
      sn_file = str_remove(deployment_full, ".csv$")
    ) %>%
    separate(date_time, into = c("date", "time"), sep = " ", remove = FALSE) %>%
    mutate(Date = mdy(date)) %>%
    mutate(Date = as.character(Date)) %>%
    separate(Date, c('year', 'month', 'day'), sep = "-", remove = FALSE) %>%
    mutate(temp_f = (temp_c * 9/5) + 32, .after = temp_c) %>%
    clean_names()
  
  # Construct SQL-ready dataframe
  aligned_data <- tbl_with_sources %>%
    transmute(
      TS_ID = NA,
      MONLOC_AB = site,
      StartDate = date,
      StartTime = time,
      DataLoggerLineName = snum,
      Deployment = deployment,
      ProjectIdentifier = "DungLowFlow",
      ActivityIdentifier = paste0(site, "_", format(mdy(date), "%Y%m%d"), "_", deployment, "_TS"),
      ActivityMediaName = str_to_title(media),
      CharacteristicName = ifelse(!is.na(do), "Dissolved oxygen (DO)", "Temperature, water"),
      ResultMeasureValue = coalesce(do, temp_c),
      ResultMeasureUnitCode = ifelse(!is.na(do), "mg/l", "deg C"),
      DataType = "Raw",
      UseForCalc = 1,
      COMMENTS = NA,
      CreatedDateTime = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      LastChangeDate = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    )
  
  return(aligned_data)
}


# Example usage
WQTS_data <- ingest_sensor_data()
print(head(WQTS_data))

