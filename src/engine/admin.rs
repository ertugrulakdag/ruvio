use super::*;

pub fn apply_connection_auth<'a>(
    security: &crate::SecurityConfig,
    command: &Command<'a>,
    auth: &mut crate::acl::ConnectionAuth,
) -> Option<Response<'static>> {
    match command {
        Command::Auth { username, password } => {
            match security.authenticate(username.as_deref().map(AsRef::as_ref), password.as_ref()) {
                Ok(user) => {
                    auth.authenticated = true;
                    auth.username = user.name.into();
                    auth.rules = std::sync::Arc::new(user.rules);
                    Some(Response::Simple(b"OK"))
                }

                Err(crate::acl::AuthError::NoPasswordConfigured) => Some(Response::Error(
                    b"ERR AUTH <password> called without any password configured",
                )),
                Err(crate::acl::AuthError::WrongPass) => {
                    Some(Response::Error(b"WRONGPASS invalid username-password pair"))
                }
            }
        }

        _ if !auth.authenticated => Some(Response::Error(b"NOAUTH Authentication required.")),
        _ => None,
    }
}

pub(crate) fn config_get_response(config: &AppConfig, pattern: &[u8]) -> Response<'static> {
    let mut array = Vec::new();

    for (name, value) in config.config_get(pattern) {
        array.push(Some(Cow::Owned(name.into_bytes())));
        array.push(Some(Cow::Owned(value.into_bytes())));
    }

    Response::Array(array)
}
