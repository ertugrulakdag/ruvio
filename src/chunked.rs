//! Append-only vector stored in fixed chunks: growing never copies existing items and never
//! leaves a freed, larger block behind, so resident memory tracks the number of items.

const CHUNK: usize = 2048;

#[derive(Debug)]
pub(crate) struct Chunked<T> {
    chunks: Vec<Vec<T>>,
    len: usize,
}

impl<T> Default for Chunked<T> {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            len: 0,
        }
    }
}

impl<T> Chunked<T> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn push(&mut self, item: T) {
        if self.chunks.last().is_none_or(|chunk| chunk.len() == CHUNK) {
            self.chunks.push(Vec::with_capacity(CHUNK));
        }

        self.chunks.last_mut().expect("chunk").push(item);
        self.len += 1;
    }
}

impl<T> std::ops::Index<usize> for Chunked<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        &self.chunks[index / CHUNK][index % CHUNK]
    }
}

impl<T> std::ops::IndexMut<usize> for Chunked<T> {
    fn index_mut(&mut self, index: usize) -> &mut T {
        &mut self.chunks[index / CHUNK][index % CHUNK]
    }
}
