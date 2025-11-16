#' Create a Client for Colombian Energy Market Data
#'
#' @description
#' Factory function to create a client for accessing Colombian energy market data
#' from either XM (SINERGOX) or SIMEM APIs. This provides a unified entry point
#' for both data sources.
#'
#' @param type Character string specifying the client type. Must be either:
#'   \itemize{
#'     \item \code{"xm"} - for XM/SINERGOX API
#'     \item \code{"simem"} - for SIMEM API
#'   }
#'
#' @return An R6 client object (\code{XMClient} or \code{SIMEMClient}) that
#'   provides methods for accessing the respective API.
#'
#' @details
#' Both client types follow the same workflow:
#' \enumerate{
#'   \item Create a client using \code{client()}
#'   \item Explore available collections using \code{$get_collections()}
#'   \item Configure a data request using \code{$config_request()}
#'   \item Retrieve data using \code{$get_data()} on the request object
#' }
#'
#' @examples
#' \dontrun{
#' # Create an XM client
#' xm <- client("xm")
#'
#' # Get available collections
#' collections <- xm$get_collections()
#'
#' # Configure a data request
#' req <- xm$config_request(
#'   collection = "Genera",
#'   metric = "CapEfec",
#'   start_date = "2024-01-01",
#'   end_date = "2024-12-31",
#'   params = list(Agente = "EMGESA")
#' )
#'
#' # Get the data
#' data <- req$get_data()
#'
#' # Same workflow for SIMEM
#' simem <- client("simem")
#' simem_collections <- simem$get_collections()
#' }
#'
#' @export
client <- function(type) {
  # Validate input
  if (missing(type) || is.null(type)) {
    cli::cli_abort(
      c(
        "Client type is required",
        "i" = "Use {.code client('xm')} or {.code client('simem')}"
      )
    )
  }

  if (!is.character(type) || length(type) != 1) {
    cli::cli_abort(
      c(
        "Client type must be a single character string",
        "i" = "Use {.code client('xm')} or {.code client('simem')}"
      )
    )
  }

  # Normalize to lowercase
  type <- tolower(trimws(type))

  # Create and return appropriate client
  if (type == "xm") {
    return(XMClient$new())
  } else if (type == "simem") {
    return(SIMEMClient$new())
  } else {
    cli::cli_abort(
      c(
        "Invalid client type: {.val {type}}",
        "i" = "Valid types are {.val xm} or {.val simem}",
        "x" = "Use {.code client('xm')} or {.code client('simem')}"
      )
    )
  }
}
