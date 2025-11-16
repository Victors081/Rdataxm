
<!-- README.md is generated from README.Rmd. Please edit that file -->

# Rdataxm

<!-- badges: start -->

<!-- badges: end -->

The goal of Rdataxm is to …

## Installation

You can install the development version of Rdataxm from
[GitHub](https://github.com/) with:

``` r
# install.packages("pak")
pak::pak("Victors081/Rdataxm")
```

## Usage

Rdataxm provides a unified interface to access Colombian energy market
data from both XM and SIMEM APIs. Both clients follow the same 4-step
workflow:

``` r
library(Rdataxm)
```

### XM Client

``` r
# Step 1: Create XM client
xm <- client("xm")

# Step 2: Explore available collections
collections <- xm$get_collections()
head(collections)

# Step 3: Configure data request
req <- xm$config_request(
  collection = "PrecBolsNaci",
  metric = "Sistema",
  start_date = "2024-01-01",
  end_date = "2024-01-31"
)

# Step 4: Get data
price_data <- req$get_data()
head(price_data)
```

### SIMEM Client

``` r
# Step 1: Create SIMEM client
simem <- client("simem")

# Step 2: Explore available variables
variables <- simem$get_collections()
head(variables)

# Step 3: Configure data request
req <- simem$config_request(
  collection = "BA1C55",  # Dataset ID
  metric = "AportesHidricosEnergia",
  start_date = "2024-01-01",
  end_date = "2024-01-31"
)

# Step 4: Get data
hydro_data <- req$get_data()
head(hydro_data)
```
