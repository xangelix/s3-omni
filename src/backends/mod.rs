#[cfg(feature = "reqwest")]
pub mod reqwest;

#[cfg(feature = "sdk")]
pub mod sdk;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub struct PresignedDownload {
    pub total_size: u64,
    pub parts: Vec<DownloadPart>,
}

impl PresignedDownload {
    /// Returns the total size of the file to be downloaded.
    #[must_use]
    pub const fn total_size(&self) -> u64 {
        self.total_size
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub struct DownloadPart {
    pub start: u64,
    pub end: u64,
    pub url: String,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub struct PresignedUpload {
    /// Will be `None` if the file fit in a single chunk (standard `PutObject`).
    pub upload_id: Option<String>,
    pub parts: Vec<UploadPart>,
}

impl PresignedUpload {
    /// Calculates the total expected size of the upload by summing the parts.
    #[must_use]
    pub fn total_size(&self) -> u64 {
        self.parts.iter().map(|p| p.expected_size).sum()
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub struct UploadPart {
    pub part_number: i32,
    pub expected_size: u64,
    pub url: String,
}
