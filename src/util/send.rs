#[cfg(not(target_family = "wasm"))]
pub trait MaybeSend: Send {}

#[cfg(not(target_family = "wasm"))]
impl<T: Send> MaybeSend for T {}

#[cfg(target_family = "wasm")]
pub trait MaybeSend {}

#[cfg(target_family = "wasm")]
impl<T> MaybeSend for T {}
