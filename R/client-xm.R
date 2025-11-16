#' XM API Client
#'
#' R6 class for accessing XM/SINERGOX API data
#'
#' @description
#' This class provides methods to connect to and retrieve data from the XM
#' (SINERGOX) API, which provides Colombian energy market data. It inherits
#' from BaseClient to provide a unified interface.
#'
#' @export
XMClient <- R6::R6Class(
  "XMClient",
  inherit = BaseClient,
  public = list(
    #' @field url Base URL for the XM API
    url = NULL,
    #' @field inventario_metricas Available metrics from the API
    inventario_metricas = NULL,

    #' @description
    #' Initialize the XM client
    #'
    #' @return A new XMClient object
    initialize = function() {
      self$client_type <- "xm"
      self$url <- "https://servapibi.xm.com.co/{period_base}"

      cli::cli_alert_info("Connecting to XM API...")
      self$inventario_metricas <- self$get_all_variables()
      cli::cli_alert_success(
        "XM client initialized with {nrow(self$inventario_metricas)} metrics available"
      )
    },

    #' @description
    #' Get all available variables from the XM API
    #'
    #' @return A data frame with all available variables
    get_all_variables = function() {
      request_body <- list(MetricId = "ListadoMetricas")

      response <- httr2::request("https://servapibi.xm.com.co/Lists") |>
        httr2::req_method("POST") |>
        httr2::req_body_json(request_body) |>
        httr2::req_perform()

      data_json <- httr2::resp_body_json(response)

      # Convert to data frame
      df_variables <- jsonlite::fromJSON(jsonlite::toJSON(data_json$Items), flatten = TRUE)

      if (nrow(df_variables) > 0) {
        df_variables <- df_variables |>
          tidyr::unnest(c(Date, ListEntities))|>
          dplyr::select(-Id, -Date) |>
          dplyr::rename_with(~ gsub("Values\\.", "", .x))
        list_cols <- sapply(df_variables, is.list)
        df_variables[list_cols] <- lapply(df_variables[list_cols], unlist)
      }

      return(df_variables)
    },

    #' @description
    #' Get collections/metrics information
    #'
    #' @param coleccion Optional parameter to filter by specific collection
    #' @return A data frame with collection information
    get_collections = function(coleccion = NULL) {
      if (is.null(coleccion)) {
        return(self$inventario_metricas)
      } else {
        if (coleccion %in% self$inventario_metricas$MetricId) {
          return(self$inventario_metricas[self$inventario_metricas$MetricId == coleccion, ])
        } else {
          cli::cli_alert_danger("Metric {coleccion} does not exist")
          return(data.frame())
        }
      }
    },

    #' @description
    #' Request data from the XM API
    #'
    #' @param coleccion Collection ID from available metrics
    #' @param metrica Metric ID from available entities
    #' @param start_date Start date in YYYY-MM-DD format
    #' @param end_date End date in YYYY-MM-DD format
    #' @param filtros Optional list of filter values
    #' @return A data frame with the requested data
    request_data = function(coleccion, metrica, start_date, end_date, filtros = NULL) {
      # Validate inputs
      if (!coleccion %in% self$inventario_metricas$MetricId) {
        cli::cli_alert_danger("Metric {coleccion} does not exist")
        return(data.frame())
      }

      if (!metrica %in% self$inventario_metricas$Entity) {
        cli::cli_alert_danger("Entity {metrica} does not exist")
        return(data.frame())
      }

      # Process filters
      if (is.null(filtros)) {
        filtros <- list()
      } else if (!is.list(filtros)) {
        cli::cli_alert_warning("Filters must be a list")
        filtros <- list()
      }

      # Convert dates
      start_date <- as.Date(start_date)
      end_date <- as.Date(end_date)

      # Generate date periods (monthly chunks)
      date_periods <- private$generate_date_periods(start_date, end_date)

      # Get entity type and configuration
      entity_info <- self$inventario_metricas[
        self$inventario_metricas$MetricId == coleccion &
        self$inventario_metricas$Entity == metrica,
      ]
      entity_type <- entity_info$Type[1]

      # Configure API endpoint based on entity type
      period_config <- private$get_period_config(entity_type)

      if (is.null(period_config)) {
        cli::cli_alert_danger("Unsupported entity type: {entity_type}")
        return(data.frame())
      }

      # Build request bodies for each period
      request_bodies <- lapply(date_periods, function(period) {
        list(
          MetricId = coleccion,
          StartDate = as.character(period$start),
          EndDate = as.character(period$end),
          Entity = metrica,
          Filter = filtros
        )
      })

      # Make requests
      if (entity_type == "ListsEntities") {
        # Handle list entities differently
        result <- private$request_lists_data(coleccion, metrica)
      } else {
        # Handle time series data
        api_url <- glue::glue("https://servapibi.xm.com.co/{period_config$period_base}")
        result <- private$request_time_series_data(request_bodies, api_url, period_config$endpoint)
      }

      # Process numeric columns
      if (nrow(result) > 0) {
        result <- private$process_numeric_columns(result)

        # Unnest all list columns to flatten the structure
        result <- result |>
          tidyr::unnest(cols = dplyr::everything())
      }

      return(result)
    }
  ),

  private = list(
    #' Generate date periods for API requests
    generate_date_periods = function(start_date, end_date) {
      # # Generate monthly periods
      # end_periods <- seq(from = start_date, to = end_date, by = "month")
      # end_periods <- lubridate::ceiling_date(end_periods, "month") - lubridate::days(1)
      #
      # # Ensure we include the actual end date
      # if (end_date > max(end_periods)) {
      #   end_periods <- c(end_periods, end_date)
      # }
      #
      # start_periods <- lubridate::floor_date(end_periods, "month")
      #
      # # Adjust first period to actual start date
      # if (start_date > start_periods[1]) {
      #   start_periods[1] <- start_date
      # }
      #
      # # Create period list
      # periods <- mapply(function(s, e) list(start = s, end = e),
      #                  start_periods, end_periods, SIMPLIFY = FALSE)
      #
      # return(periods)
      list(list(start = start_date, end = end_date))
    },

    #' Get period configuration for entity type
    get_period_config = function(entity_type) {

      if (!is.character(entity_type) || length(entity_type) != 1) {
        cli::cli_alert_danger("Invalid entity_type: must be a single character string")
        return(NULL)
      }

      configs <- list(
        "HourlyEntities" = list(period_base = "hourly", endpoint = "HourlyEntities"),
        "DailyEntities" = list(period_base = "daily", endpoint = "DailyEntities"),
        "MonthlyEntities" = list(period_base = "monthly", endpoint = "MonthlyEntities"),
        "AnnualEntities" = list(period_base = "annual", endpoint = "AnnualEntities"),
        "ListsEntities" = list(period_base = "lists", endpoint = "ListEntities")
      )

      return(configs[[entity_type]])
    },

    #' Request time series data
    request_time_series_data = function(request_bodies, api_url, endpoint) {
      all_data <- list()

      for (i in seq_along(request_bodies)) {
        tryCatch({
          response <- httr2::request(api_url) |>
            httr2::req_method("POST") |>
            httr2::req_body_json(request_bodies[[i]]) |>
            httr2::req_perform()

          data_json <- httr2::resp_body_json(response)

          if (!is.null(data_json$Items) && length(data_json$Items) > 0) {
            df_chunk <- jsonlite::fromJSON(jsonlite::toJSON(data_json$Items), flatten = TRUE)
            if (nrow(df_chunk) > 0) {
              # Detectar columna de valores (distinta de Date)
              value_col <- setdiff(names(df_chunk), "Date")[[1]]

              # Extraer fechas planas
              fechas <- unlist(df_chunk$Date)

              # Extraer subdata.frames y combinar
              valores_df <- do.call(
                rbind,
                lapply(df_chunk[[value_col]], function(x) {
                  # Convertir a data.frame plano
                  as.data.frame(x)
                })
              )

              # Renombrar columnas quitando "Values."
              colnames(valores_df) <- gsub("^Values\\.", "", colnames(valores_df))

              # Construir tibble final con fechas
              df_chunk <- tibble::tibble(Date = as.Date(fechas), !!!valores_df)
              all_data[[i]] <- df_chunk
            }

            # if (nrow(df_chunk) > 0) {
            #   # Clean column names
            #   names(df_chunk) <- gsub(paste0(endpoint, "\\."), "", names(df_chunk))
            #   all_data[[i]] <- df_chunk
            # }
          }
        }, error = function(e) {
          cli::cli_alert_warning("Error in request {i}: {e$message}")
        })
      }

      if (length(all_data) > 0) {
        result <- do.call(rbind, all_data)
        #result <- result[!duplicated(result), ]
      } else {
        result <- data.frame()
      }

      return(result)
    },

    #' Request lists data
    request_lists_data = function(coleccion, metrica) {
      request_body <- list(
        MetricId = coleccion,
        Entity = metrica
      )

      response <- httr2::request("https://servapibi.xm.com.co/lists") |>
        httr2::req_method("POST") |>
        httr2::req_body_json(request_body) |>
        httr2::req_perform()

      data_json <- httr2::resp_body_json(response)

      if (!is.null(data_json$Items) && length(data_json$Items) > 0) {
        result <- jsonlite::fromJSON(jsonlite::toJSON(data_json$Items), flatten = TRUE)
        names(result) <- gsub("ListEntities\\.", "", names(result))
      } else {
        result <- data.frame()
      }

      return(result)
    },

    #' Process numeric columns
    process_numeric_columns = function(df) {
      # Convert appropriate columns to numeric
      for (col in names(df)) {
        if (col != "Date" && col != "date" && col!= "Id" && col!="code" && col!="Name") {
          df[[col]] <- suppressWarnings(as.numeric(df[[col]]))
        }
      }

      # Process date columns
      if ("Date" %in% names(df)) {
        df$Date <- as.Date(df$Date, format = "%Y-%m-%d")
      }
      if ("date" %in% names(df)) {
        df$date <- as.Date(df$date, format = "%Y-%m-%d")
      }

      return(df)
    }
  )
)
