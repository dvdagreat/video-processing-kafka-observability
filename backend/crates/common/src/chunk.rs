use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkMeta {
    pub video_id: Uuid,
    pub chunk_index: u32,
    pub is_last: bool,
    pub resolution: Option<String>,
}

impl ChunkMeta {
    pub fn to_headers(&self) -> BTreeMap<String, Vec<u8>> {
        let mut headers = BTreeMap::new();
        headers.insert("video_id".to_string(), self.video_id.to_string().into_bytes());
        headers.insert(
            "chunk_index".to_string(),
            self.chunk_index.to_string().into_bytes(),
        );
        headers.insert("is_last".to_string(), self.is_last.to_string().into_bytes());
        if let Some(resolution) = &self.resolution {
            headers.insert("resolution".to_string(), resolution.clone().into_bytes());
        }
        headers
    }

    pub fn from_headers(headers: &BTreeMap<String, Vec<u8>>) -> Result<Self> {
        let get = |key: &str| -> Result<String> {
            headers
                .get(key)
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .ok_or_else(|| anyhow!("missing kafka header {key}"))
        };
        Ok(Self {
            video_id: Uuid::parse_str(&get("video_id")?)?,
            chunk_index: get("chunk_index")?.parse()?,
            is_last: get("is_last")?.parse()?,
            resolution: headers
                .get("resolution")
                .map(|v| String::from_utf8_lossy(v).into_owned()),
        })
    }
}
