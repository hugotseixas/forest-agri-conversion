# HEADER ----------------------------------------------------------------------
#
# Title:        Extract conversion from Forest to Agriculture
# Description:  This code extracts values of the conversion between Forest and
#               Agriculture from MapBiomas raster files. The process is applied
#               in each file, where a number of iterations are performed to
#               extract values from different conversion cycles. The results
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
library(terra)
library(fs)
library(magrittr)
library(glue)
library(arrow)
library(readr)
library(tidyr)
library(stringr)
library(tibble)
library(purrr)
library(dplyr)
#
# OPTIONS ---------------------------------------------------------------------
#
source("conf/config.R")
#
# LOAD AUXILIARY DATA ---------------------------------------------------------

## Read MapBiomas classes information ----
mb_dict <- read_delim('data/mb_class_dictionary.csv', delim = ',')

## Load mask cells from parquet dataset  ----
mask_ds <- open_dataset("data/c_tabular_dataset/mask_cells/")

# LIST RASTER FILES -----------------------------------------------------------

## Create a list of files for each raster type ----
file_list <-
  dir_info(
    path = 'data/raw_raster_tiles/',
    regexp = "mb_lulc.+tif$"
  ) %>%
  select(path) %>%
  mutate(
    tile_code = str_extract(path, "(?<=mb_lulc-)[^.tif]+")
  ) %>%
  mutate(tile_id = row_number()) %>%
  relocate(tile_id, tile_code) %>%
  arrange(tile_code)

# EXTRACT LULC CONVERSION VALUES ----------------------------------------------

## Iterate trough files to extract conversion values ----
walk(
  .x = pull(.data = file_list, var = tile_id),
  function(tile) {

    ### Load one file as a raster stack ----
    mb_raster <- rast(x = file_list$path[tile])

    cat('\n')

    cat(
      'Extracting from tile:',
      tile,
      '-',
      file_list$path[tile],
      '\n',
      sep = ' '
    )

    ## Get cells from mask for the tile ----
    mask_subset <-
      mask_ds %>%
      filter(tile_id == tile) %>%
      select(cell_id, tile_cell_id) %>%
      collect()

    ## Convert raster stack to tibble ----
    lulc_table <-
      mb_raster %>%
      as.data.frame(cells = TRUE) %>%
      as_tibble() %>%
      pivot_longer(
        cols = starts_with("classification"),
        names_to = "year",
        values_to = "class_code"
      ) %>%
      filter(class_code != 0) %>%
      mutate(year = as.integer(str_extract(year, "(\\d)+"))) %>%
      rename(tile_cell_id = cell) %>%
      left_join(mask_subset, by = "tile_cell_id") # Get cell_id from mask

    ## Check if raster have any value != 0 ----
    if (nrow(lulc_table) == 0) {

      cat(' --- Raster have no relevant data', '\n')

    } else {

      ### Create empty tibble to store conversion values ----
      c_length_all <-
        tibble(
          cell_id = integer(),
          agri_code = integer(),
          forest_year = integer(),
          agri_year = integer(),
          c_length = integer(),
          c_cycle = integer()
        )

      ### Create empty tibble to store lulc time series ----
      c_classes_all <-
        tibble(
          cell_id = integer(),
          year = integer(),
          class_code = integer(),
          c_cycle = integer()
        )

      ### Start conversion cycles counting  ----
      cycle <- 0L

      cat(' ---- Conversion cycle: ')

      ### Get conversion length to each conversion cycle ----
      repeat {

        cycle <- cycle + 1L

        cat(cycle, ' ')

        #### Get first year of anthropic cover ----
        first_atpc_year <-
          lulc_table %>%
          left_join(mb_dict, by = 'class_code') %>%
          group_by(cell_id) %>%
          filter(class_type == 'anthropic') %>%
          summarise(year = min(year), .groups = 'drop') %>%
          rename(atpc_year = year)

        ### Get years of forest after 'first_atpc_year' ----
        first_forest_year <-
          lulc_table %>%
          left_join(first_atpc_year, by = 'cell_id') %>%
          group_by(cell_id) %>%
          filter(year > atpc_year & class_code %in% trans_nat) %>%
          rename(forest_year = year)

        ### Check forest existence after 'first_atpc_year' ----
        if (nrow(first_forest_year) > 0) {

          #### Get first year of forest after 'first_atpc_year' ----
          first_forest_year %<>%
            summarise(forest_year = min(forest_year), .groups = 'drop')

          #### Divide lulc_table into before/after 'first_forest_year' ----
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

          #### Set all rows as period = "Before" ----
          lulc_table %<>%
            mutate(period = 'before')

        }

        ### Extract period before first forest after anthropic cover ----
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

        ### Check if conditions above return any data ----
        if (nrow(before) > 0) {

          #### Classify cells into agriculture and forest ----
          before %<>%
            mutate(
              class_type = case_when(
                class_code %in% trans_ant ~ 'agri',
                class_code %in% trans_nat ~ 'forest'
              )
            ) %>%
            drop_na()

          #### Get the max year of forest in the "before" period ----
          forest_max <-
            before %>%
            filter(class_type == 'forest') %>%
            group_by(cell_id) %>%
            slice(which.max(year)) %>%
            rename(forest_max = year) %>%
            mutate(forest_max = forest_max + 1) # Get deforestation year

          #### Get the min year of agriculture in the "before" period ----
          agri_min <-
            before %>%
            filter(class_type == 'agri') %>%
            group_by(cell_id) %>%
            slice(which.min(year)) %>%
            rename(agri_min = year) %>%
            select(-c('period', 'class_type'))

          #### Calculate change length ----
          c_length <-
            agri_min %>%
            left_join(
              forest_max %>% select(cell_id, forest_max),
              by = 'cell_id'
            ) %>%
            mutate(
              c_length = agri_min - forest_max,
              c_cycle = cycle
            ) %>%
            ungroup() %>%
            arrange(cell_id)

          #### Filter years inside conversion and 5 years after ----
          c_classes <-
            lulc_table %>%
            select(-period) %>%
            left_join(
              c_length %>%
                select(cell_id, agri_min, forest_max, c_cycle),
              by = 'cell_id',
            ) %>%
            arrange(cell_id) %>%
            filter(year > forest_max, year <= (agri_min + 5)) %>%
            select(cell_id, year, class_code, c_cycle)

          #### Append data to "c_classes_all" ----
          c_classes_all %<>%
            bind_rows(c_classes)

          #### Rename and organize columns of conversion length table ----
          c_length %<>%
            rename(
              agri_code = class_code,
              forest_year = forest_max,
              agri_year = agri_min
            ) %>%
            mutate(
              forest_year = forest_year,
              agri_year = agri_year
            ) %>%
            select(
              cell_id, agri_code, forest_year,
              agri_year, c_length, c_cycle
            )

          #### Append data to "c_length_all" ----
          c_length_all %<>%
            bind_rows(c_length)

        }

        ### Filter the period "after" ----
        # Filter only the years that occurred after the period analyzed above
        # This will update the table for the new iteration
        lulc_table %<>%
          filter(period == 'after') %>%
          select(-period)

        # Check if there is any data in the updated version of the table
        if (nrow(lulc_table) == 0) {

          break

        }

        # Get the classes remaining in the table
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

      ## Calculate agri_cycle and forest_type for c_length_all ----
      c_length_all %<>%
        group_by(cell_id) %>%
        mutate(agri_cycle = row_number()) %>%
        ungroup() %>%
        mutate(
          forest_type = as.integer(
            case_when(
              c_cycle == 1 ~ 1, # Primary forest
              c_cycle > 1 ~ 2 # Secondary forest
            )
          )
        )

      ## Calculate agri_cycle for c_classes_all ----
      c_classes_all %<>%
        left_join(
          c_length_all %>%
            select(cell_id, c_cycle, agri_cycle),
          by = c("cell_id", "c_cycle")
        ) %>%
        select(-c_cycle)

      ## Create directories and store tables as parquet files ----
      walk2(
        .x = c("c_length", "c_classes"),
        .y = list(c_length_all, c_classes_all),
        function(var, table) {

          # Check if table have any row
          if (nrow(table) > 0) {

            # Create directories and save files for each agri_cycle value ----
            walk(
              unique(table$agri_cycle),
              function(cycle) {

                # Create directories ----
                dir_create(
                  path = glue(
                    "data/c_tabular_dataset/{var}/",
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
                    "data/c_tabular_dataset/{var}/",
                    "tile_id={tile}/",
                    "agri_cycle={cycle}/",
                    "{var}-{file_list$tile_code[tile]}.parquet"
                  ),
                  version = "2.6"
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

# Work complete!
try(beepr::beep(4), silent = TRUE)
