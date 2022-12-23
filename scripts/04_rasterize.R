# HEADER ----------------------------------------------------------------------
#
# Title:        Create conversion raster files
# Description:  This codes takes values from the "c_length" dataset and
#               metadata information to recreate raster tiles with the
#               conversion values. The new raster files share the same spatial
#               characteristics as the downloaded tiles (extent, resolution...)
#               Each tile will be composed by a set of raster files, one file
#               for each forest-agriculture conversion cycle, and the bands of
#               the new files carry values of the variables stored in the
#               parquet dataset.
#
# Author:       Hugo Tameirao Seixas
# Contact:      seixas.hugo@protonmail.com
# Date:         2020-09-11
#
# Notes:        Do not change the names of files and directories of the
#               results, since it will break the next codes.
#
# LIBRARIES -------------------------------------------------------------------
#
library(terra)
library(magrittr)
library(fs)
library(glue)
library(arrow)
library(tibble)
library(dplyr)
library(purrr)
#
# OPTIONS ---------------------------------------------------------------------
#

#
# GET TILES LIST --------------------------------------------------------------

## Get tiles list from metadata file ----
tiles_metadata <-
  read_parquet("data/c_tabular_dataset/tiles_metadata.parquet")

# RASTERIZE TABLES ------------------------------------------------------------

## Create directory to store new raster files  ----
dir_create("data/c_raster_tiles/")

## Load conversion length values from parquet dataset  ----
c_ds <- open_dataset("data/c_tabular_dataset/c_length/")

## Load mask cells from parquet dataset  ----
mask_ds <- open_dataset("data/c_tabular_dataset/mask_cells/")

## Create set of raster files for each tile ----
walk(
  .x = tiles_metadata$tile_id,
  function(tile) {

    cat('\n')

    cat('Creating raster from tile:', tile, '\n', sep = ' ')

    # Get cells from mask for the tile ----
    cells <-
      mask_ds %>%
      filter(tile_id == tile) %>%
      select(cell_id, tile_cell_id) %>%
      collect()

    # Get metadata for the tile ----
    meta <-
      tiles_metadata %>%
      filter(tile_id == tile)

    # Get conversion values ----
    c_table <-
      c_ds %>%
      filter(tile_id == tile) %>% # First filter the dataset for the tile
      select(-tile_id) %>%
      group_by(agri_cycle) %>%
      collect()

    # Check if there is any valid value ----
    # Avoids possible unexpected errors
    if (nrow(c_table) == 0) { return() }

    # Create one raster for each group (agri_cycle) ----
    c_table %>%
      group_walk(
        ~ {

          ##### Set values for all raster cells ----
          c_table <-
            .x %>%
            # Get cell id of the tile
            full_join(cells, by = "cell_id") %>%
            # Expand values to the whole raster extent
            # Will include NA values for empty cells (very important)
            right_join(
              tibble(tile_cell_id = c(1:meta$n_cell)),
              by = "tile_cell_id"
            ) %>%
            arrange(tile_cell_id)

          # Get variables names ----
          variables <-
            c_table %>%
            select(agri_code:forest_type) %>%
            names()

          # Create a list of raster layers and their values ----
          c_raster_list <-
            map(
              .x = variables,
              function(var) {

                #### Create empty raster using metadata values ----
                c_raster <-
                  rast(
                    extent = ext(
                      c(meta$x_min, meta$x_max, meta$y_min, meta$y_max)
                    ),
                    ncols = meta$n_col,
                    nrows = meta$n_row,
                    crs = meta$crs
                  )

                # Arrange cell values ----
                raster_values <-
                  c_table %>%
                  arrange(tile_cell_id) %>%
                  pull(!!var)

                # Fill layer with values ----
                c_raster <- setValues(c_raster, values = raster_values)

                # Set layer name ----
                names(c_raster) <- var

                return(c_raster)

              }
            )

          # Create raster stack from list ----
          c_stack <- rast(c_raster_list)

          # Save stack to file ----
          writeRaster(
            c_stack,
            glue(
              'data/c_raster_tiles/',
              'c_stack_{meta$tile_code}_cycle_{.y}.tif'
            ),
            wopt = list(
              datatype = 'INT2U',
              progress = 0
            ),
            overwrite = TRUE
          )

        }
      )

  }
)

# Work complete!
try(beepr::beep(4), silent = TRUE)
