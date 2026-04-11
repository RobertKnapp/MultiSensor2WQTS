# Multi-sensor2WQTS

## Overview
**Multi-sensor2WQTS** is an R-based data management pipeline designed to ingest water quality data from multiple makes and models of data logging sondes. The primary goal of this project is to process and format environmental sensor data so it can be saved to a SQL Server database called WQTS (Water Quality Time Series).

## Supported Sensors
The initial version of this project provides code and sample data tailored specifically for **HOBO® DO / Temperature data loggers (Model U26-001)**. The architecture of the pipeline is built to be extensible, with plans to add support for additional sensor types in the future.

## Features
* **Interactive Project Configuration**: Launches an interactive data editor interface (via `DataEditR`) to set default project values such as `ProjectIdentifier` and `OrganizationIdentifier` (e.g., "JSKTRIBE_WQX") before writing them to the database.
* **Automated Metadata Extraction**: Extracts the sensor's serial number directly from the header lines of the CSV files (specifically looking for the pattern `S/N: (\d+)`).
* **Filename Parsing**: Automatically extracts metadata including the site, media type, deployment year, and deployment season by parsing the filenames of the imported CSVs.
* **Data Cleaning & Transformation**: 
    * Separates datetime strings into individual date and time columns.
    * Converts imported Celsius temperature readings into Fahrenheit.
    * Maps dissolved oxygen and temperature values into a standard `CharacteristicName` format ("Dissolved oxygen (DO)" or "Temperature, water") along with their respective units ("mg/l" or "deg C").
* **Database Alignment**: Constructs a SQL-ready dataframe matching the WQTS schema, including fields like `TS_ID`, `MONLOC_AB`, `DataLoggerLineName`, `ActivityIdentifier`, and automated system timestamps.

## Prerequisites
To run this script, you will need **RStudio** (required for the interactive folder selection feature) and the following R packages installed:
* `tidyverse` (includes `dplyr`, `stringr`, `purrr`, `readr`)
* `lubridate`
* `janitor`
* `rstudioapi`
* `DataEditR`
* `tibble`
* `DBI` & `odbc` (for SQL Server connections)
* `readxl` & `writexl` (for handling Excel outputs)

## Workflow & Usage
1. **Initialize Project Parameters**: Run the script to open the interactive data editor. Fill in project details and close the editor to save the row to the WQTS `Project` table.
2. **Select Sensor Type**: The console will output a mapped list of available sensor types (currently mapping `HOBO_U26-001_DO_Temp`). Enter the corresponding number to select your sensor.
3. **Select Data Folder**: An interactive RStudio dialog box will prompt you to select the directory containing your raw data files.
4. **Data Ingestion (`ingest_sensor_data`)**: The script will automatically search the selected folder for `.csv` files, parse the files (skipping the first two header rows), and compile the data. 
5. **Output**: The finalized, SQL-aligned dataset is previewed in the console and written to a local Excel file named `WQTS_data.xlsx`.

### Important: File Naming Convention
For the automated metadata extraction to work properly, your `.csv` files must follow a specific naming structure separated by underscores. The script parses filenames into the following components: `site_media_deployment.csv`. 

## Acknowledgments
The WQTS database was developed and provided by the **NW Indian Fisheries Commission**. Students and faculty from the **University of Washington Department of Statistics** contributed to the development of this project. 

Robert Knapp <robert.jskt@gmail.com> 
