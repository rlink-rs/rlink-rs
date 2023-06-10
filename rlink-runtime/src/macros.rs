#[macro_export]
macro_rules! loop_fn {
    ($expr:expr, $dur:expr) => {{
        let mut rt_wrap = None;
        loop {
            let fn_rt = $expr;
            match fn_rt {
                std::result::Result::Ok(val) => {
                    // always true. just to avoid code checking
                    // eg => warn: value assigned to `rt_wrap` is never read
                    if rt_wrap.is_none() {
                        rt_wrap = Some(val);
                    }
                    break;
                }
                std::result::Result::Err(err) => {
                    error!("loop function({}) error. {}", stringify!($expr), err);
                    tokio::time::sleep($dur).await;
                }
            }
        }
        rt_wrap.unwrap()
    }};
}

// const DUR_2S: std::time::Duration = std::time::Duration::from_secs(2);
// const DUR_500MS: std::time::Duration = std::time::Duration::from_millis(500);
// const DUR_50MS: std::time::Duration = std::time::Duration::from_millis(50);
// const DUR_5MS: std::time::Duration = std::time::Duration::from_millis(5);

#[macro_export]
macro_rules! async_delay {
    ($delay:expr) => {{
        if $idle_counter > 3 {
            if $idle_counter < 10 {
                tokio::time::delay_for(std::time::Duration::MILLISECOND).await;
            } else if $idle_counter < 20 {
                tokio::time::delay_for(std::time::Duration::from_millis(50)).await;
            } else if $idle_counter < 30 {
                tokio::time::delay_for(std::time::Duration::from_millis(500)).await;
            } else {
                tokio::time::delay_for(std::time::Duration::from_secs(2)).await;
            }
        }
    }};
}

#[cfg(test)]
mod tests {
    use std::borrow::BorrowMut;

    fn mock(value: &mut i64) -> Result<String, std::io::Error> {
        println!("======");
        *value += 1;
        if *value < 100 {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "a"))
        } else {
            Ok("V".to_string())
        }
    }

    #[tokio::test]
    pub async fn loop_result_test() {
        let mut value = 0i64;
        let n: String = loop_fn!(
            mock(value.borrow_mut()),
            std::time::Duration::from_millis(5)
        );

        assert_eq!(n, "V");
        assert_eq!(value, 100i64);
    }
}
