/// RAII Guard to guarantee progress bars are properly incremented and completed.
/// This prevents orphaned progress bars during panics or early error returns.
pub struct ProgressGuard(pub Option<atomic_progress::Progress>);

impl ProgressGuard {
    /// Safely increments the progress bar if it exists.
    #[inline]
    pub fn inc(&self, amount: u64) {
        if let Some(pb) = &self.0 {
            pb.inc(amount);
        }
    }
}

impl Drop for ProgressGuard {
    fn drop(&mut self) {
        if let Some(pb) = &self.0 {
            pb.finish();
        }
    }
}
