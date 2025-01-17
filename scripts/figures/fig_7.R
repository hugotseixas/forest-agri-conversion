# HEADER ----------------------------------------------------------------------
#
# Title:        Create figure 7
# Description:  The objective of this routine is compare the conversion length
#               results, comparing them with visual observations.
#
#
# Author:       Hugo Tameirao Seixas
# Contact:      seixas.hugo@protonmail.com
# Date:         2022-08-03
#
# Notes:
#
#
#
#
# LIBRARIES -------------------------------------------------------------------
#
library(arrow)
library(magrittr)
library(lubridate)
library(glue)
library(dplyr)
library(units)
library(tibble)
library(readr)
library(tidyr)
library(forcats)
library(purrr)
library(scales)
library(ggplot2)
library(ggtext)
#
# OPTIONS ---------------------------------------------------------------------
#
#
# LOAD DATA -------------------------------------------------------------------

## Load observed data ----
observed <- read_csv("data/validation/results.csv") %>%
  mutate( # Remove points where there were no conversion
    forest_year = if_else(forest_year <= 0, NA_real_, forest_year),
    agri_year = if_else(agri_year <= 0, NA_real_, agri_year),
    c_length = if_else(agri_year <= 0, NA_real_, c_length),
    c_length = if_else(forest_year <= 0, NA_real_, c_length)
  ) %>%
  mutate( # Fix dates to match the estimates
    forest_year = forest_year,
    agri_year = agri_year
  ) %>%
  pivot_longer(
    cols =  c(forest_year, agri_year, c_length),
    names_to = "var",
    values_to = "observed"
  ) %>%
  arrange(cell_id)

## Load estimates data ----
estimated <- open_dataset("data/c_tabular_dataset/c_length/")

estimated <- estimated %>%
  filter(
    cell_id %in% observed$cell_id
  ) %>%
  collect() %>%
  pivot_longer(
    cols =  c(forest_year, agri_year, c_length),
    names_to = "var",
    values_to = "estimated"
  ) %>%
  arrange(cell_id)

# Check if there is any difference between tables
setdiff(unique(observed$cell_id), unique(estimated$cell_id))

## Join tables ----
comparison <- estimated %>%
  inner_join(observed, by = c("cell_id", "var")) %>%
  mutate(
    difference = observed - estimated,
    var = factor(
      var,
      levels = c("forest_year", "agri_year", "c_length"),
      labels = c(
        "Deforestation", "Agriculture Establishment", "Conversion Length"
      )
    )
  )

## Calculate Mean Average Error ----
mae <- comparison %>%
  group_by(var) %>%
  summarise(mae = mean(abs(difference), na.rm = TRUE)) %>%
  add_row(mae = .$mae * -1, var = .$var)

## Calculate bias and pbias ----
bias <- comparison %>%
  group_by(var) %>%
  summarise(
    bias = mean(difference, na.rm = TRUE),
    pbias =
      sum(difference, na.rm = TRUE) / sum(abs(observed), na.rm = TRUE) * 100
  )

## Create table with metrics ----
metrics <- mae %>%
  filter(mae > 0) %>%
  full_join(bias, by = "var")

## Save plots data ----

# Comparison table
write_csv(comparison, "data/figures/fig_7_8.csv")

# Error metrics
write_csv(metrics, "data/figures/fig_7_metrics.csv")

# ANALYSE RESULTS -------------------------------------------------------------

## Create bar plot with the errors ----
bar_plot <- comparison %>%
  ggplot() +
  facet_wrap( ~ var) +
  ggtitle("Distribution of the errors of conversion estimates.") +
  geom_vline(
    data = mae,
    aes(xintercept = mae),
    color = "white", linetype = "dashed"
  ) +
  geom_bar(aes(x = difference), fill = "black") +
  geom_text(
    data = metrics,
    size = 2.3,
    color = "#f7f7f7",
    aes(
      x = -28,
      y = 53,
      hjust = 0,
      fontface = "bold",
      label = glue(
        "MAE = {round(mae, 2)}",
        "\n",
        "BIAS = {round(bias, 2)}",
        "\n",
        "PBIAS = {round(pbias, 2)}"
      )
    )
  ) +
  geom_text(
    data = tibble(
      label = c("(a)", "(b)", "(c)"),
      var = factor(
        c("Deforestation", "Agriculture Establishment", "Conversion Length")
      ),
      x = rep(25, 3),
      y = rep(57, 3)
    ),
    aes(x = x, y = y, label = label),
    size = 3,
    color = "white"
  ) +
  labs(x = "Error", y = "Count") +
  coord_fixed() +
  theme_dark() +
  theme(
    text = element_text(size = 11),
    axis.text.y = element_text(angle = 45, vjust = 0.6, hjust = 0.5),
    axis.text.x = element_text(angle = 45, vjust = 0.6, hjust = 0.4),
    legend.position = ""
  ) +
  guides(
    fill = guide_colourbar(
      barwidth = 10,
      barheight = 0.8,
      title.vjust = 1
    ),
    color = "none"
  )

ggsave(
  "./figs/error_bars.png",
  bar_plot,
  width = 17,
  height = 8.5,
  units = "cm",
  dpi = 600
)

