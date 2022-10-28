# HEADER ----------------------------------------------------------------------
#
# Title:        Validate transitions against visual inspection
# Description:  The objective of this routine is compare the transition length
#               results, comparing them with visual observations.
#
#
# Author:       Hugo Tameirao Seixas
# Contact:      tameirao.hugo@gmail.com
# Date:         2022-08-03
#
# Notes:
#
#
#
#
# LIBRARIES -------------------------------------------------------------------
#
library(sf)
library(terra)
library(magrittr)
library(fs)
library(lubridate)
library(glue)
library(arrow)
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
  mutate( # Remove points where there were no transition
    forest_year = if_else(forest_year <= 0, NA_real_, forest_year),
    agri_year = if_else(agri_year <= 0, NA_real_, agri_year),
    trans_length = if_else(agri_year <= 0, NA_real_, trans_length),
    trans_length = if_else(forest_year <= 0, NA_real_, trans_length)
  ) %>%
  mutate( # Fix dates to match the estimates
    forest_year = forest_year - 1984 - 1, # - 1 is for last year of forest
    agri_year = agri_year - 1984
  ) %>%
  pivot_longer(
    cols =  c(forest_year, agri_year, trans_length),
    names_to = "var",
    values_to = "observed"
  ) %>%
  arrange(cell_id)

## Load estimates data ----
estimated <- open_dataset("data/trans_tabular_dataset/trans_length/")

estimated <- estimated %>%
  filter(
    cell_id %in% observed$cell_id
  ) %>%
  collect() %>%
  pivot_longer(
    cols =  c(forest_year, agri_year, trans_length),
    names_to = "var",
    values_to = "estimated"
  ) %>%
  arrange(cell_id)

# Check if there is any difference between tables
setdiff(unique(observed$cell_id), unique(estimated$cell_id))

## Join tables ----
comparison <- estimated %>%
  inner_join(observed, by = c("cell_id", "var")) %>%
  mutate(difference = observed - estimated)

# ANALYSE RESULTS -------------------------------------------------------------

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

## Create bar plot with the errors ----
bar_plot <- comparison %>%
  ggplot() +
  facet_wrap( ~ var) +
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
  glue("./figs/error_bars.png"),
  bar_plot,
  width = 17,
  height = 7.5,
  units = "cm",
  dpi = 600
)

quants <- comparison %>%
  group_by(var) %>%
  summarise(
    value = quantile(
      difference,
      probs = c(0.05, 0.25, 0.75, 0.95),
      na.rm = TRUE
    )
  ) %>%
  mutate(
    lim = c("min", "min", "max", "max"),
    group_id = c(1, 2, 2, 1)
  ) %>%
  pivot_wider(
    id_cols = c(var, group_id),
    names_from = lim,
    values_from = value
  )

quants <- do.call("rbind", replicate(37, quants, simplify = FALSE)) %>%
  group_by(var, group_id) %>%
  mutate(estimated = 0:36, observed = 0:36)

## Create scatter plot with the errors ----
scatter_plot <- comparison %>%
  ggplot() +
  facet_wrap( ~ var) +
  geom_ribbon(
    data = subset(quants, group_id == 2),
    aes(
      ymin = observed + min, ymax = max + observed,
      xmin = estimated + min, xmax = max + estimated,
      x = estimated, y = observed
    ),
    fill = "white", alpha = 0.7
  ) +
  geom_ribbon(
    data = subset(quants, group_id == 1),
    aes(
      ymin = observed + min, ymax = max + observed,
      xmin = estimated + min, xmax = max + estimated,
      x = estimated, y = observed
    ),
    fill = "white", alpha = 0.3
  ) +
  geom_abline(intercept = 0, slope = 1, color = "#6a6a6a") +
  geom_text(
    data = tibble(
      quant = c("5%", "25%", "75%", "95%"),
      observed = c(17 + 12, 17 + 4.75, 17 - 1.75, 17 - 8.7),
      estimated = c(16.5, 16.5, 16.5, 16.5),
      var = factor("agri_year")
    ),
    aes(x = estimated, y = observed, label = quant),
    hjust = "left",
    size = 2.3,
    fontface = "bold"
  ) +
  geom_point(
    aes(x = estimated, y = observed),
    fill = "black",
    size = 0.6
  ) +
  scale_x_continuous(breaks = pretty_breaks(n = 5), expand = c(0, 0)) +
  scale_y_continuous(breaks = pretty_breaks(n = 5), expand = c(0.01, 0.01)) +
  coord_fixed(
    ylim = c(0, 36), xlim = c(0, 36),
    expand = TRUE,
    clip = "on"
  ) +
  labs(x = "Estimated", y = "Observed") +
  theme_dark() +
  theme(
    text = element_text(size = 11),
    axis.text.y = element_text(angle = 45, vjust = 0.6, hjust = 0.5),
    axis.text.x = element_text(angle = 45, vjust = 0.6, hjust = 0.4),
    legend.position = "bottom"
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
  glue("./figs/error_scatter.png"),
  scatter_plot,
  width = 17,
  height = 7,
  units = "cm",
  dpi = 600
)
