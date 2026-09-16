use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Result;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

#[derive(Default)]
pub struct WriterState {
    pub next_index: u32,
    pending: BTreeMap<u32, (Vec<u8>, bool)>,
    file: Option<File>,
}

impl WriterState {
    pub async fn accept(&mut self, path: &Path, index: u32, is_last: bool, bytes: Vec<u8>) -> Result<bool> {
        if index != self.next_index {
            self.pending.insert(index, (bytes, is_last));
            return Ok(false);
        }

        self.write(path, bytes).await?;
        self.next_index += 1;
        let mut completed = is_last;

        while let Some((next_bytes, next_is_last)) = self.pending.remove(&self.next_index) {
            self.write(path, next_bytes).await?;
            self.next_index += 1;
            completed = next_is_last;
        }

        if completed {
            self.file = None;
        }
        Ok(completed)
    }

    async fn write(&mut self, path: &Path, bytes: Vec<u8>) -> Result<()> {
        if self.file.is_none() {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)
                .await?;
            self.file = Some(file);
        }
        self.file.as_mut().unwrap().write_all(&bytes).await?;
        Ok(())
    }
}
