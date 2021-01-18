pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error {
    pub(crate) inner_error: anyhow::Error,
}

impl Error {
    pub fn msg(msg: String) -> Self {
        Error {
            inner_error: anyhow!(msg),
        }
    }

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

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error { inner_error: e }
    }
}
