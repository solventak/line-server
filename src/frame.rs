use std::convert::TryFrom;
use std::fmt;

#[derive(Debug)]
pub enum FrameError {
    // TODO: is this the right way to make errors?  do we still want to call it frame error?
    // TODO: make this error more general
    InvalidChecksum,
    // You can add other variants here for other types of errors
    LineIndexOutOfBounds, // TODO: should lineindexoutofbounds be a separate "requesterror" type or something?
    ParseError, // TODO: definitely need to review the structure of this error enum... there's gotta be a better way to do this
    ClientDisconnected,
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::InvalidChecksum => write!(f, "Invalid checksum"),
            // Handle other variants here
            FrameError::LineIndexOutOfBounds => write!(f, "Line index out of bounds"),
            FrameError::ParseError => write!(f, "Parse error"),
            FrameError::ClientDisconnected => write!(f, "Client disconnected"),
        }
    }
}

impl std::error::Error for FrameError {}

pub enum Command {
    Get(u32),
    Quit,
    Shutdown,
}

impl TryFrom<&[u8]> for Command {
    type Error = FrameError;

    fn try_from(value: &[u8]) -> std::prelude::v1::Result<Self, Self::Error> {
        // TODO: do some assertions that the value[0] is in valid ascii range
        match value[0] as char {
            // TODO: do we want to validate the args before this point?
            '0' => {
                let line_number = u32::from_be_bytes([value[1], value[2], value[3], value[4]]);
                Ok(Command::Get(line_number))
            }
            '1' => Ok(Command::Quit),
            '2' => Ok(Command::Shutdown),
            _ => Err(FrameError::ParseError),
        }
    }
}

impl Command {
    fn as_bytes(&self) -> Vec<u8> {
        match self {
            Command::Get(line_number) => {
                let mut bytes = vec!['0' as u8]; // TODO: clean this up maybe... this is ugly
                bytes.extend_from_slice(&line_number.to_be_bytes());
                bytes
            }
            Command::Quit => vec!['1' as u8],
            Command::Shutdown => vec!['2' as u8],
        }
    }
}

pub struct Frame {
    pub cmd: Command,
    checksum: u8, // TODO could make this smaller? not sure how checksums are normally done
}

impl Frame {
    fn validate_checksum(&self) -> bool {
        let cmd_bytes = self.cmd.as_bytes();
        let mut checksum: u32 = 0;
        for byte in cmd_bytes.iter() {
            checksum += *byte as u32;
        }
        checksum = checksum as u32 % 256 as u32; // TODO: these conversions are ugly
                                                 // println!("{checksum} == {}", self.checksum);
        checksum as u8 == self.checksum
    }
}

impl TryFrom<&[u8]> for Frame {
    // TODO return some sort of data invalid error
    type Error = FrameError;

    fn try_from(value: &[u8]) -> std::prelude::v1::Result<Self, Self::Error> {
        // created frame
        // validate_checksum of the created frame
        // validate that GET has non-zero and other commands have 0x00
        match value.len() {
            0 => Err(FrameError::ClientDisconnected),
            7 => {
                let command = Command::try_from(&value[0..5])?;
                let checksum = u8::from_be_bytes([value[5]]);
                let frame = Frame {
                    cmd: command,
                    checksum,
                };
                if !frame.validate_checksum() {
                    return Err(FrameError::InvalidChecksum);
                }
                Ok(frame)
            }
            _ => Err(FrameError::ParseError),
        }
    }
}