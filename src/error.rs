#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    BinProtError(binprot::Error),
    IncorrectPayloadLength(i64),
    NoMagicNumberInHandshake,
    UnexpectedMagicNumber(i64),
    OneshotError(tokio::sync::oneshot::error::RecvError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IoError(e)
    }
}

impl From<binprot::Error> for Error {
    fn from(e: binprot::Error) -> Self {
        Error::BinProtError(e)
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(e: tokio::sync::oneshot::error::RecvError) -> Self {
        Error::OneshotError(e)
    }
}
