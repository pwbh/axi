use std::fmt::Display;

use crate::{offset::Offset, MAX_BATCH_SIZE};

#[derive(PartialEq)]
pub enum BatchState {
    ShouldFlush,
    Allowable,
}

#[derive(Debug)]
pub struct Prune<'a> {
    pub buffer: &'a [u8],
    pub offsets: &'a [Offset],
}

impl<'a> Prune<'a> {
    pub fn offsets_as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.offsets.as_ptr() as *const u8,
                self.offsets.len() * std::mem::size_of::<Offset>(),
            )
        }
    }

    pub fn buffer_as_bytes(&self) -> &[u8] {
        &self.buffer[..]
    }
}

#[derive(Debug)]
pub struct Batch {
    buffer: [u8; MAX_BATCH_SIZE],
    offsets: Vec<Offset>,
    current_batch_size: usize,
    current_batch_index: usize,
    current_segment_size: usize,
    current_segment_count: usize,
}

impl Batch {
    pub fn new() -> Self {
        Self {
            buffer: [0; MAX_BATCH_SIZE],
            offsets: Vec::with_capacity(1024),
            current_batch_size: 0,
            current_batch_index: 0,
            current_segment_size: 0,
            current_segment_count: 0,
        }
    }

    pub fn add(
        &mut self,
        key: &str,
        buf: &[u8],
        latest_segment_count: usize,
        latest_segment_size: usize,
    ) -> Result<BatchState, String> {
        if self.current_batch_size + buf.len() < MAX_BATCH_SIZE {
            if latest_segment_count != self.current_segment_count {
                self.current_segment_count = latest_segment_size;
                self.current_segment_size = 0;
            }

            let offset = Offset::new(
                key,
                self.current_segment_size,
                self.current_segment_size + buf.len(),
                latest_segment_count,
            )?;

            self.buffer[self.current_batch_size..self.current_batch_size + buf.len()]
                .copy_from_slice(buf);
            self.current_batch_size += buf.len();
            self.offsets.push(offset);
            self.current_batch_index += 1;
            self.current_segment_size += buf.len();
            Ok(BatchState::Allowable)
        } else {
            Ok(BatchState::ShouldFlush)
        }
    }

    pub fn reset(&mut self) {
        self.offsets.clear();
        self.current_batch_size = 0;
        self.current_batch_index = 0;
    }

    pub fn get_prunable(&self) -> Prune<'_> {
        Prune {
            buffer: &self.buffer[..self.current_batch_size],
            offsets: &self.offsets[..],
        }
    }
}

impl Display for Batch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#"
current_batch_size: {},
current_batch_index: {},
current_segment_size: {},
current_segmetn_count: {}
        "#,
            self.current_batch_size,
            self.current_batch_index,
            self.current_batch_size,
            self.current_segment_count
        )
    }
}
