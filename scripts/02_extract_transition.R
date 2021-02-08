# HEADER ----------------------------------------------------------------------
#
# Title:        Extract transition from Forest to Agriculture
# Description:  This code extracts values of the transition between Forest and
#               Agriculture from MapBiomas raster files. The process is applied
#               in each file, where a number of iterations are performed to
#               extract values from different transition cycles. The results
#               are saved in .parquet format.
#
# Author:       Hugo Tameirao Seixas
# Contact:      tameirao.hugo@gmail.com
# Date:         2020-09-11
#
# Notes:        Do not change the names of files and directories of the
#               results, since it will break the next codes.
#
# LIBRARIES -------------------------------------------------------------------
#
library(sf)
library(raster)
library(tabularaster)
library(geobr)
library(fs)
library(magrittr)
library(glue)
library(arrow)
library(tidyverse)
#
# OPTIONS ---------------------------------------------------------------------
#
source('conf/config.R')
sf::sf_use_s2(TRUE)
#
# LOAD AUXILIARY DATA ---------------------------------------------------------

## Read MapBiomas classes information ----
mb_dict <- read_delim('data/mb_class_dictionary.csv', delim = ',')

## Load municipalities vector data ----
municip <-
  read_municipality(year = "2019") %>%
  filter(code_state %in% c(11, 12, 13, 14, 15, 16, 17, 21, 51)) %>%
  select(code_muni, geom)

# LIST RASTER FILES -----------------------------------------------------------

## Create a list of files for each raster type ----
file_list <-
  map(
    .x = c("mb_lulc", "mb_mask"),
    function(raster_type) {

      return(
        dir_info(
          path = 'data/raw_raster_tiles/',
          regexp = glue("{raster_type}.+tif$")
        ) %>%
          select(path) %>%
          mutate(
            tile_code = str_extract(path, glue("(?<={raster_type}-)[^.tif]+"))
          ) %>%
          rename(!!raster_type := path)
      )

    }
  )

## Join the file tables ----
file_list <-
  reduce(
    .x = file_list,
    .f = left_join,
    by = "tile_code"
  ) %>%
  mutate(tile_id = row_number()) %>%
  relocate(tile_id, tile_code) %>%
  arrange(tile_code)

# EXTRACT MASK VALUES ---------------------------------------------------------

## Extract mask values into a single table ----
mask_table <-
  map2_df(
    .x = pull(.data = file_list, var = mb_mask),
    .y = pull(.data = file_list, var = tile_id),
    function(raster_path, tile) {

      # Extract values of a single tile ----
      return(
        stack(raster_path) %>%
          tabularaster::as_tibble(xy = TRUE, cell = TRUE) %>%
          rename(tile_cell_id = cellindex) %>%
          filter(!(cellvalue == 0)) %>%
          pivot_wider(names_from = dimindex, values_from = cellvalue) %>%
          rename(area = `2`) %>%
          select(-`1`) %>%
          mutate(tile_id = tile)
      )

    }
  )

## Create unique cell_id for the whole mask ----
mask_table %<>%
  mutate(cell_id = row_number()) %>%
  rename(lon = x, lat = y) %>%
  relocate(cell_id, tile_id, tile_cell_id)

# EXTRACT LULC TRANSITION VALUES ----------------------------------------------

## Create empty table to store metadata ----
tiles_metadata <-
  tibble(
    tile_id = integer(),
    tile_code = character(),
    x_min = double(),
    x_max = double(),
    y_min = double(),
    y_max = double(),
    n_col = integer(),
    n_row = integer(),
    n_cell = integer(),
    crs = character()
  )

## Iterate trough files to extract transition values ----
walk(
  .x = pull(.data = file_list, var = tile_id),
  function(tile) {

    # Load one file as a raster stack ----
    mb_raster <- stack(x = file_list$mb_lulc[tile])

    cat('\n')

    cat(
      'Extracting from tile:',
      tile,
      '-',
      file_list$mb_lulc[tile],
      '\n',
      sep = ' '
    )

    # Add row to raster metadata table ----
    tiles_metadata %<>%
      add_row(
        tile_id = tile,
        tile_code = file_list$tile_code[tile],
        x_min = raster::xmin(mb_raster),
        x_max = raster::xmax(mb_raster),
        y_min = raster::ymin(mb_raster),
        y_max = raster::ymax(mb_raster),
        n_col = raster::ncol(mb_raster),
        n_row = raster::nrow(mb_raster),
        n_cell = raster::ncell(mb_raster),
        crs = as.character(raster::crs(mb_raster))
      )

    # Get the mask data for tile ----
    mask_subset <-
      mask_table %>%
      filter(tile_id == tile)

    # Convert raster stack to tibble ----
    lulc_table <-
      mb_raster %>%
      tabularaster::as_tibble() %>%
      filter(cellvalue != 0) %>%
      rename(
        class_code = cellvalue,
        tile_cell_id = cellindex,
        year = dimindex
      ) %>%
      left_join(mask_subset, by = "tile_cell_id") %>% # Get cell_id from mask
      select(-c(lon, lat, area))

    # Check if raster have any value != 0 ----
    if (nrow(lulc_table) == 0) {

      cat(' --- Raster have no relevant data', '\n')

    } else {

      #### Create empty tibble to store transition values ----
      trans_length_all <-
        tibble(
          cell_id = integer(),
          agri_code = integer(),
          forest_year = integer(),
          agri_year = integer(),
          trans_length = integer(),
          trans_cycle = integer()
        )

      # Create empty tibble to store lulc time series ----
      trans_classes_all <-
        tibble(
          cell_id = integer(),
          year = integer(),
          class_code = integer(),
          trans_cycle = integer()
        )

      # Start transition cycles counting  ----
      cycle <- 0L

      cat(' ---- Transition cycle: ')

      # Get transition length to each transition cycle ----
      repeat {

        cycle <- cycle + 1L

        cat(cycle, ' ')

        ## Get first year of anthropic cover ----
        first_atpc_year <-
          lulc_table %>%
          left_join(mb_dict, by = 'class_code') %>%
          group_by(cell_id) %>%
          filter(class_type == 'anthropic') %>%
          summarise(year = min(year), .groups = 'drop') %>%
          rename(atpc_year = year)

        ## Get years of forest after 'first_atpc_year' ----
        first_forest_year <-
          lulc_table %>%
          left_join(first_atpc_year, by = 'cell_id') %>%
          group_by(cell_id) %>%
          filter(year > atpc_year & class_code %in% trans_nat) %>%
          rename(forest_year = year)

        ## Check forest existence after 'first_atpc_year' ----
        if (nrow(first_forest_year) > 0) {

          ## Get first year of forest after 'first_atpc_year' ----
          first_forest_year %<>%
            summarise(forest_year = min(forest_year), .groups = 'drop')

          ## Divide lulc_table into before/after 'first_forest_year' ----
          lulc_table %<>%
            left_join(first_forest_year, by = 'cell_id') %>%
            mutate(
              period = case_when(
                year < forest_year ~ 'before',
                year >= forest_year ~ 'after',
                is.na(forest_year) ~ 'before'
              )
            ) %>%
            select(-forest_year)

        } else {

          ## Set all rows as period = "Before" ----
          lulc_table %<>%
            mutate(period = 'before')

        }

        # Extract period before first forest after anthropic cover ----
        before <-
          lulc_table %>%
          filter(period == 'before') %>%
          group_by(cell_id) %>%
          # Filter cells which contains agriculture AND forest classes
          filter(
            all(
              any(class_code %in% trans_ant),
              any(class_code %in% trans_nat)
            )
          ) %>%
          ungroup()

        # Check if conditions above return any data ----
        if (nrow(before) > 0) {

          ## Classify cells into agriculture and forest ----
          before %<>%
            mutate(
              class_type = case_when(
                class_code %in% trans_ant ~ 'agri',
                class_code %in% trans_nat ~ 'forest'
              )
            ) %>%
            drop_na()

          ## Get the max year of forest in the "before" period ----
          forest_max <-
            before %>%
            filter(class_type == 'forest') %>%
            group_by(cell_id) %>%
            slice(which.max(year)) %>%
            rename(forest_max = year)

          ## Get the min year of agriculture in the "before" period ----
          agri_min <-
            before %>%
            filter(class_type == 'agri') %>%
            group_by(cell_id) %>%
            slice(which.min(year)) %>%
            rename(agri_min = year) %>%
            select(-c('period', 'class_type'))

          ## Calculate change length ----
          trans_length <-
            agri_min %>%
            left_join(
              forest_max %>% select(cell_id, forest_max),
              by = 'cell_id'
            ) %>%
            mutate(
              trans_length = agri_min - forest_max,
              trans_cycle = cycle
            ) %>%
            ungroup() %>%
            arrange(cell_id)

          ## Filter years inside transition and 5 years after ----
          trans_classes <-
            lulc_table %>%
            select(-period) %>%
            left_join(
              trans_length %>%
                select(cell_id, agri_min, forest_max, trans_cycle),
              by = 'cell_id',
            ) %>%
            arrange(cell_id) %>%
            filter(year > forest_max, year <= (agri_min + 5)) %>%
            select(cell_id, year, class_code, trans_cycle)

          ## Append data to "trans_classes_all" ----
          trans_classes_all %<>%
            bind_rows(trans_classes)

          ## Rename and organize columns of trans length table ----
          trans_length %<>%
            rename(
              agri_code = class_code,
              forest_year = forest_max,
              agri_year = agri_min,
            ) %>%
            select(
              cell_id, agri_code, forest_year,
              agri_year, trans_length, trans_cycle
            )

          ## Append data to "trans_length_all" ----
          trans_length_all %<>%
            bind_rows(trans_length)

        }

        # Filter the period "after" ----
        # Filter only the years that occurred after the period analyzed above
        # This will update the table for the new iteration
        lulc_table %<>%
          filter(period == 'after') %>%
          select(-period)

        # Check if there is any data in the update version of the table
        if (nrow(lulc_table) == 0) {

          break

        }

        dist_list <- lulc_table %>% distinct(class_code) %>% pull(class_code)

        # Check if there is more classes than just forest in the updated table
        #if (sum(distinct_list) <= 3) {

        #  break

        #}

        # Check if there is forest AND agriculture in the updated table
        if (!(trans_nat %in% dist_list) || !(any(trans_ant %in% dist_list))) {

          break

        }

      }

      # Calculate agri_cycle and forest_type for trans_length_all ----
      trans_length_all %<>%
        group_by(cell_id) %>%
        mutate(agri_cycle = row_number()) %>%
        ungroup() %>%
        mutate(
          forest_type = as.integer(
            case_when(
              trans_cycle == 1 ~ 1, # Primary forest
              trans_cycle > 1 ~ 2 # Secondary forest
            )
          )
        )

      # Calculate agri_cycle for trans_classes_all ----
      trans_classes_all %<>%
        left_join(
          trans_length_all %>%
            select(cell_id, trans_cycle, agri_cycle),
          by = c("cell_id", "trans_cycle")
        ) %>%
        select(-trans_cycle)

      # Add municipality in which each pixel is within ----
      mask_subset %<>%
        select(-tile_id) %>%
        st_as_sf(coords = c("lon", "lat"), crs = crs(mb_raster)) %>%
        st_join(
          municip %>% st_transform(crs(mb_raster)),
          join = st_within
        ) %>%
        mutate(coords = st_as_text(geometry)) %>%
        as_tibble() %>%
        select(-geometry)

      # Create directories and store tables as parquet files ----
      walk2(
        .x = c("mask_cells", "trans_length", "trans_classes"),
        .y = list(mask_subset, trans_length_all, trans_classes_all),
        function(var, table) {

          # Create directories for mask tables  ----
          if (var == "mask_cells" & nrow(table) > 0) {

            ## Create directories ----
            dir_create(
              path = glue(
                "data/trans_tabular_dataset/{var}/",
                "tile_id={tile}/"
              )
            )

            # Write files ----
            write_parquet(
              x = table,
              sink = glue(
                "data/trans_tabular_dataset/{var}/",
                "tile_id={tile}/",
                "{var}-{file_list$tile_code[tile]}.parquet"
              ),
              version = "2.0"
            )

          } else if (nrow(table) > 0) {

            # Create directories and save files for each agri_cycle value ----
            walk(
              unique(table$agri_cycle),
              function(cycle) {

                # Create directories ----
                dir_create(
                  path = glue(
                    "data/trans_tabular_dataset/{var}/",
                    "tile_id={tile}/",
                    "agri_cycle={cycle}/"
                  )
                )

                # Write files ----
                write_parquet(
                  x = table %>%
                    filter(agri_cycle == cycle) %>%
                    select(-agri_cycle),
                  sink = glue(
                    "data/trans_tabular_dataset/{var}/",
                    "tile_id={tile}/",
                    "agri_cycle={cycle}/",
                    "{var}-{file_list$tile_code[tile]}.parquet"
                  ),
                  version = "2.0"
                )

              }
            )

          }

        }
      )

      cat('\n')

    }

  }
)

## Save raster metadata file ----
write_parquet(
  x = tiles_metadata,
  sink = "data/trans_tabular_dataset/tiles_metadata.parquet",
  version = "2.0"
)

# Work complete!
try(beepr::beep(4), silent = TRUE)
