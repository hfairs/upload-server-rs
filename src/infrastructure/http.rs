use axum::http::StatusCode;

use crate::domain::value_objects::Error;

/// Builds an HTTP error response tuple from a status code and a displayable error.
pub fn rest_error(status: StatusCode, err: impl std::fmt::Display) -> (StatusCode, String) {
    (status, err.to_string())
}

/// Maps domain [`Error`] variants to HTTP status codes.
pub fn into_rest_error(err: Error) -> (StatusCode, String) {
    match err {
        // 4xx errors
        Error::ValidationError(msg) => (StatusCode::BAD_REQUEST, msg),
        Error::AuthenticationError(msg) => (StatusCode::UNAUTHORIZED, msg),
        Error::AuthorizationError(msg) => (StatusCode::FORBIDDEN, msg),
        Error::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
        Error::AlreadyExistsError(msg) => (StatusCode::CONFLICT, msg),
        // 5xx errors
        other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_validation_to_400() {
        let (status, _) = into_rest_error(Error::ValidationError("bad".into()));
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn maps_not_found_to_404() {
        let (status, msg) = into_rest_error(Error::NotFound("gone".into()));
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(msg, "gone");
    }

    #[test]
    fn maps_internal_to_500() {
        let (status, _) = into_rest_error(Error::InternalError("oops".into()));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }
}
