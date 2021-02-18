# CONFIG ----------------------------------------------------------------------
#
# This is a configuration file that sets options and variables that will be
# used in the "01_download_mapbiomas.R" and "02_extract_transition.R".
# The options "gee_email", "clear_driver_folder" and "view_map" are safe
# to be changed ("gee_email" is actually essential to run the download script).
# However, it is not guaranteed that changes in the other options will generate
# valid results, changing them may even break the codes. They are available
# here to facilitate testing and future developments.

# Set your GEE email ----------------------------------------------------------
gee_email <- ""
# Insert your email between the quotes
# You must be registered in Google Earth Engine (GEE) with a Gmail account
# To have access to the google drive to download the data
# GEE home page:
# https://earthengine.google.com/

# Preview masks result in GEE before download ---------------------------------
view_map <- FALSE
# This will create the plot and ask if it should proceed
# Default:
#   view_map = FALSE

# Set the biome to download data from -----------------------------------------
biome <- 1
# Default:
#   biome <- 1

## The option must be a value from 1 to 6
## Each number represents the following biome:
##
## 1  ------  Amazônia
## 2  ------  Caatinga
## 3  ------  Cerrado
## 4  ------  Mata Atlântica
## 5  ------  Pampa
## 6  ------  Pantanal
##

# The spatial resolution of the download --------------------------------------
scale <- 30
# Default:
#   scale <- 30

# The dimension of tiles to be downloaded from GEE ----------------------------
tile_dim <- 1536L
# Values have to be multiple of 256
# Values ALWAYS have to be followed by the letter L
# You can use tile_dim <- NULL to let GEE to choose the size automatically
# Bigger values will result in bigger tiles, demanding more memory
# Default:
#   tile_dim <- 1280L

# The natural class of the transition -----------------------------------------
trans_nat <- c(3)
# Default value is "Forest Formation":
#   trans_nat <- c(3)
# Check https://mapbiomas.org/codigos-de-legenda?cama_set_language=en

# The non natural class of the transition -------------------------------------
trans_ant <- c(18, 19, 39, 20, 41, 36)
# Default value are agriculture classes:
#   trans_ant <- c(18, 19, 39, 20, 41, 36)
# Check https://mapbiomas.org/codigos-de-legenda?cama_set_language=en
