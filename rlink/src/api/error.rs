pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error {
    pub(crate) inner_error: anyhow::Error,
}

impl<'a> From<&'a str> for Error {
    fn from(msg: &'a str) -> Self {
        Error {
            inner_error: anyhow!(msg.to_string()),
        }
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Error {
            inner_error: anyhow!(msg),
        }
    }
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error { inner_error: e }
    }
}

impl Error {
    pub fn wrap<E>(e: E) -> Self
    where
        E: std::error::Error + Into<anyhow::Error>,
    {
        Error {
            inner_error: anyhow!(e),
        }
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner_error.fmt(f)
    }
}
