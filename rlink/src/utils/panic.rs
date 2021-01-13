use std::sync::atomic::{AtomicBool, Ordering};

static PANIC_CAPTURE: AtomicBool = AtomicBool::new(false);

pub fn is_panic() -> bool {
    PANIC_CAPTURE.load(Ordering::SeqCst)
}

pub fn panic_notify() {
    // std::panic::set_hook(Box::new(|panic_info| {
    //     PANIC_CAPTURE.store(true, Ordering::SeqCst);
    //
    //     eprintln!("{:?}", backtrace::Backtrace::new());
    //     if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
    //         eprintln!("panic occurred: {:?}", s);
    //     } else {
    //         eprintln!("panic occurred");
    //     }
    //
    //     if let Some(location) = panic_info.location() {
    //         eprintln!(
    //             "panic occurred in file '{}' at line {}",
    //             location.file(),
    //             location.line()
    //         );
    //     } else {
    //         eprintln!("panic occurred but can't get location information...");
    //     }
    // }));
}
