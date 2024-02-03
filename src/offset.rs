pub const MAX_KEY_SIZE: usize = 128;

#[derive(Debug, Clone, PartialEq)]
#[repr(C)]
pub struct Offset {
    key_size: usize,
    key: [u8; MAX_KEY_SIZE],
    start: usize,
    data_size: usize,
    segment_count: usize,
}

impl Offset {
    pub fn new(k: &str, start: usize, end: usize, segment_count: usize) -> Result<Self, String> {
        if k.len() > MAX_KEY_SIZE {
            return Err(format!("Provided `key` value exceeds maximum length of {} bytes. Please make your key shorter.", MAX_KEY_SIZE));
        }

        if start >= end {
            return Err(format!(
                "Start ({}) can't be greater or equal to end ({})",
                start, end
            ));
        }

        let mut key = [0; MAX_KEY_SIZE];
        let key_slice = &mut key[..k.len()];
        key_slice.copy_from_slice(k.as_bytes());

        Ok(Self {
            key,
            key_size: k.len(),
            start,
            data_size: end - start,
            segment_count,
        })
    }

    pub fn from(k: &str, start: usize, data_size: usize, segment_count: usize) -> Self {
        let mut key = [0; MAX_KEY_SIZE];
        let key_slice = &mut key[..k.len()];
        key_slice.copy_from_slice(k.as_bytes());

        Self {
            key,
            key_size: k.len(),
            start,
            data_size,
            segment_count,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        let offset = self as *const _ as *const [u8; std::mem::size_of::<Offset>()];
        unsafe { &(*offset) }
    }

    pub fn start(&self) -> usize {
        self.start
    }

    pub fn data_size(&self) -> usize {
        self.data_size
    }

    pub fn segment_count(&self) -> usize {
        self.segment_count
    }

    pub fn key(&self) -> String {
        std::str::from_utf8(&self.key[..self.key_size])
            .unwrap()
            .to_string()
    }
}
