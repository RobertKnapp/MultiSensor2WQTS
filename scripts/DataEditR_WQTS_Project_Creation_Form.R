
library(DataEditR)
library(tibble)

# Create one row with default values
initial_data <- tibble(
  ProjectIdentifier = "",
  ProjectName = "",
  ProjectDescriptionText = "",
  OrganizationIdentifier = "JSKTRIBE_WQX",  # Default value
  LastChangeDate = Sys.Date()              # Today's date
)

# Launch the editor
edited_data <- data_edit(initial_data)

# View the result
print(edited_data)

# Load Libraries ----------------------------------------------------------

library(DBI)                # for db connection

# Connect to WQTS ---------------------------------------------------------
# Connect to WQTS
con <- dbConnect(odbc::odbc(),"twqd_test",
                 Database = "WQTS")

# Read/write Project Table ------------------------------------------------
# Write Data into Project Table in TWQD
dbWriteTable(con, "Project", edited_data, append = TRUE, row.names = FALSE)

project <- dbGetQuery(con, "select * from Project")

