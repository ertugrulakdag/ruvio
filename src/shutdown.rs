use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Clone, Debug, Default)]
pub struct ShutdownToken {
    requested: Arc<AtomicBool>,
}

impl ShutdownToken {
    pub fn request(&self) {
        self.requested.store(true, Ordering::Release);
    }

    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_observe_shutdown_requests() {
        let token = ShutdownToken::default();
        let observer = token.clone();

        token.request();
        assert!(observer.is_requested());
    }
}
