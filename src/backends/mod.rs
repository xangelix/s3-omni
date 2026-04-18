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
