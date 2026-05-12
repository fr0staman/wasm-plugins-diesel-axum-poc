use axum::{
    Json,
    extract::State,
    http::StatusCode,
    middleware,
    response::{IntoResponse, Response},
};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};

use crate::{api::AppState, auth};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    /// user_id as a string (JWT `sub` field convention).
    pub sub: String,
    pub tenant_id: u64,
    pub role: String,
    /// Unix timestamp expiry.
    pub exp: u64,
}

#[derive(Clone)]
pub struct AuthConfig {
    pub decoding_key: DecodingKey,
    pub encoding_key: EncodingKey,
    pub validation: Validation,
}

impl AuthConfig {
    pub fn from_secret(secret: &str) -> Self {
        Self {
            decoding_key: DecodingKey::from_secret(secret.as_bytes()),
            encoding_key: EncodingKey::from_secret(secret.as_bytes()),
            validation: Validation::default(), // HS256, validates exp
        }
    }

    pub fn verify(&self, token: &str) -> jsonwebtoken::errors::Result<Claims> {
        decode(token, &self.decoding_key, &self.validation).map(|t| t.claims)
    }

    pub fn sign(&self, claims: &Claims) -> jsonwebtoken::errors::Result<String> {
        encode(&Header::default(), claims, &self.encoding_key)
    }
}

/// Extract the raw token from an `Authorization: Bearer <token>` header value.
pub fn bearer_token(authorization: &str) -> Option<&str> {
    authorization.strip_prefix("Bearer ").map(str::trim)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn far_future_exp() -> u64 {
        9_999_999_999 // year ~2286, always valid
    }

    fn past_exp() -> u64 {
        1 // Unix epoch + 1s, always expired
    }

    fn test_claims(exp: u64) -> Claims {
        Claims {
            sub: "42".into(),
            tenant_id: 1,
            role: "admin".into(),
            exp,
        }
    }

    // ── bearer_token ──────────────────────────────────────────────────────────

    #[test]
    fn bearer_token_valid_header() {
        assert_eq!(bearer_token("Bearer mytoken123"), Some("mytoken123"));
    }

    #[test]
    fn bearer_token_trims_whitespace() {
        assert_eq!(bearer_token("Bearer  spaced "), Some("spaced"));
    }

    #[test]
    fn bearer_token_lowercase_bearer_rejected() {
        assert_eq!(bearer_token("bearer mytoken"), None);
    }

    #[test]
    fn bearer_token_basic_scheme_rejected() {
        assert_eq!(bearer_token("Basic dXNlcjpwYXNz"), None);
    }

    #[test]
    fn bearer_token_empty_string() {
        assert_eq!(bearer_token(""), None);
    }

    #[test]
    fn bearer_token_no_space_after_bearer() {
        // "Bearer" without trailing space — strip_prefix("Bearer ") fails
        assert_eq!(bearer_token("BearerNoSpace"), None);
    }

    // ── AuthConfig round-trip ─────────────────────────────────────────────────

    #[test]
    fn sign_and_verify_round_trip() {
        let cfg = AuthConfig::from_secret("super-secret");
        let claims = test_claims(far_future_exp());
        let token = cfg.sign(&claims).expect("signing failed");
        let decoded = cfg.verify(&token).expect("verification failed");
        assert_eq!(decoded.sub, "42");
        assert_eq!(decoded.tenant_id, 1);
        assert_eq!(decoded.role, "admin");
    }

    #[test]
    fn verify_with_wrong_secret_fails() {
        let signer = AuthConfig::from_secret("correct-secret");
        let verifier = AuthConfig::from_secret("wrong-secret");
        let token = signer.sign(&test_claims(far_future_exp())).unwrap();
        assert!(verifier.verify(&token).is_err());
    }

    #[test]
    fn verify_expired_token_fails() {
        let cfg = AuthConfig::from_secret("secret");
        let token = cfg.sign(&test_claims(past_exp())).unwrap();
        assert!(cfg.verify(&token).is_err());
    }

    #[test]
    fn verify_garbage_token_fails() {
        let cfg = AuthConfig::from_secret("secret");
        assert!(cfg.verify("not.a.jwt").is_err());
    }

    #[test]
    fn verify_empty_string_fails() {
        let cfg = AuthConfig::from_secret("secret");
        assert!(cfg.verify("").is_err());
    }
}

pub async fn layer_require_auth(
    State(app): State<AppState>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    let auth_header = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let Some(token) = auth_header.and_then(auth::bearer_token) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "missing token"})),
        )
            .into_response();
    };

    let Ok(claims) = app.auth.verify(token) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "invalid token"})),
        )
            .into_response();
    };

    let mut req = request;
    req.extensions_mut().insert(claims);
    next.run(req).await
}
