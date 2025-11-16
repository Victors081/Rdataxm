#' Base Client for Data APIs
#'
#' @description
#' Abstract base class for XM and SIMEM API clients. This class provides
#' shared functionality and defines the interface that all client types must implement.
#'
#' @details
#' This class should not be instantiated directly. Use the \code{client()} factory
#' function to create XM or SIMEM client instances.
#'
BaseClient <- R6::R6Class(
  "BaseClient",
  public = list(
    #' @field client_type Character string identifying the client type ("xm" or "simem")
    client_type = NULL,

    #' @description
    #' Configure a data request
    #'
    #' @param collection Collection or dataset ID
    #' @param metric Metric or variable ID
    #' @param start_date Start date in YYYY-MM-DD format
    #' @param end_date End date in YYYY-MM-DD format
    #' @param params Optional named list of additional filter parameters
    #' @return A DataRequest object configured with the specified parameters
    config_request = function(collection, metric, start_date, end_date, params = NULL) {
      # Validate required parameters
      if (missing(collection) || is.null(collection)) {
        cli::cli_abort("Parameter {.arg collection} is required")
      }
      if (missing(metric) || is.null(metric)) {
        cli::cli_abort("Parameter {.arg metric} is required")
      }
      if (missing(start_date) || is.null(start_date)) {
        cli::cli_abort("Parameter {.arg start_date} is required")
      }
      if (missing(end_date) || is.null(end_date)) {
        cli::cli_abort("Parameter {.arg end_date} is required")
      }

      # Validate date parameters
      tryCatch({
        start_date <- as.Date(start_date)
        end_date <- as.Date(end_date)
      }, error = function(e) {
        cli::cli_abort("Invalid date format. Use YYYY-MM-DD format")
      })

      if (start_date > end_date) {
        cli::cli_abort("Start date must be before or equal to end date")
      }

      # Validate params if provided
      if (!is.null(params) && !is.list(params)) {
        cli::cli_abort("Parameter {.arg params} must be a named list")
      }

      # Create and return DataRequest object
      DataRequest$new(
        client = self,
        collection = collection,
        metric = metric,
        start_date = start_date,
        end_date = end_date,
        params = params
      )
    },

    #' @description
    #' Get available collections/datasets
    #'
    #' @param collection Optional collection ID to filter results
    #' @return Data frame with collection information
    #'
    #' @details
    #' This is an abstract method that must be implemented by child classes.
    get_collections = function(collection = NULL) {
      cli::cli_abort(
        "{.fn get_collections} must be implemented by child class"
      )
    }
  ),

  private = list(
    #' Validate that a collection exists
    #'
    #' @param collection Collection ID to validate
    #' @param available_collections Data frame of available collections
    #' @param id_column Name of the ID column in available_collections
    #' @return Logical indicating whether collection is valid
    validate_collection = function(collection, available_collections, id_column) {
      if (!id_column %in% names(available_collections)) {
        cli::cli_abort("Invalid collection data structure")
      }

      collection %in% available_collections[[id_column]]
    },

    #' Validate that a metric exists
    #'
    #' @param metric Metric ID to validate
    #' @param available_metrics Character vector of available metrics
    #' @return Logical indicating whether metric is valid
    validate_metric = function(metric, available_metrics) {
      metric %in% available_metrics
    }
  )
)
