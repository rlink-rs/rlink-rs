pub fn get_percentile_capacity(scale: &'static [f64]) -> usize {
    (scale.len() + 1) << 3
}

pub struct Percentile<'a> {
    // accumulate counter
    // count: u64,
    // scale boundary slice
    scale: &'static [f64],
    // value statistics vec
    count_container: &'a mut [u8],

    counter_index: usize,
}

impl<'a> Percentile<'a> {
    pub fn new(scale: &'static [f64], count_container: &'a mut [u8]) -> Self {
        Percentile {
            // count: 0,
            scale,
            count_container,
            counter_index: scale.len() << 3,
        }
    }

    #[inline]
    fn read(&self, index: usize) -> u64 {
        let c = [
            self.count_container[index],
            self.count_container[index + 1],
            self.count_container[index + 2],
            self.count_container[index + 3],
            self.count_container[index + 4],
            self.count_container[index + 5],
            self.count_container[index + 6],
            self.count_container[index + 7],
        ];

        u64::from_be_bytes(c)
    }

    #[inline]
    fn write(&mut self, index: usize, value: u64) {
        let c = value.to_be_bytes();

        self.count_container[index] = c[0];
        self.count_container[index + 1] = c[1];
        self.count_container[index + 2] = c[2];
        self.count_container[index + 3] = c[3];
        self.count_container[index + 4] = c[4];
        self.count_container[index + 5] = c[5];
        self.count_container[index + 6] = c[6];
        self.count_container[index + 7] = c[7];
    }

    #[inline]
    fn get_counter(&self) -> u64 {
        self.read(self.counter_index)
    }

    fn incr_counter(&mut self) {
        let c = self.read(self.counter_index);
        self.write(self.counter_index, c + 1);
    }

    pub fn accumulate(&mut self, value: f64) {
        self.incr_counter();

        let index = self.position_in_value_array(value);
        match index {
            Some(index) => {
                // index << 3): index * 8
                let begin_index = index << 3;

                let mut n = self.read(begin_index);
                n += 1u64;

                self.write(begin_index, n);
            }
            None => {}
        }
    }

    /// find index by value boundary
    fn position_in_value_array(&self, val: f64) -> Option<usize> {
        let length = self.scale.len();

        if val >= self.scale[length - 1] {
            return Some(length - 1);
        } else if val <= self.scale[0] {
            return Some(0);
        }

        self.search(val)
    }

    fn search(&self, target: f64) -> Option<usize> {
        let nums = self.scale;

        let n = nums.len();
        let mut i = 0;
        let mut j = n;
        // let mut mid = 0;
        while i < j {
            let mid = (i + j) >> 1;
            let mid_value = nums[mid];
            if mid_value == target {
                return Some(mid);
            } else if target > mid_value {
                // turn right
                if target < nums[mid + 1] {
                    return Some(mid + 1);
                }

                i = mid + 1;
            } else {
                // turn left
                if mid == 0 {
                    return Some(mid);
                }
                if target > nums[mid - 1] {
                    return Some(mid);
                }

                j = mid;
            }
        }
        return None;
    }

    pub fn get_result(&self, water_line: u8) -> f64 {
        if water_line > 100 {
            panic!("waterLine must be less than 100.0 and more than 0.0")
        }

        let percent_right_value = (100 - water_line) as f64 / 100f64;
        let counter = self.get_counter();

        // fault tolerance
        let percent_xx_pos = self.adjust((counter as f64 * percent_right_value) as u64, counter);
        let mut percent_xx_value = f64::MAX;

        let mut scanned = 0;

        // -1): skip latest counter filed
        let length = self.scale.len();

        let mut index = length;
        while index > 0 {
            index -= 1;

            // index << 3): index * 8
            let scale_counter = self.read(index << 3);

            // no value, skip
            if 0u64 == scale_counter {
                continue;
            }

            scanned += scale_counter;

            // water line check
            if scanned >= percent_xx_pos {
                percent_xx_value = self.scale[index];
                break;
            }
        }

        return percent_xx_value;
    }

    /// fault tolerance，ensure that values are within [1, MAX] range after water line process
    fn adjust(&self, input: u64, max: u64) -> u64 {
        return if input <= 1 {
            1
        } else if input >= max {
            max
        } else {
            input
        };
    }

    pub fn merge(&mut self, percentile: &Percentile) {
        for i in 0..self.count_container.len() {
            self.count_container[i] += percentile.count_container[i];
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::functions::percentile::{get_percentile_capacity, Percentile};
    use std::sync::Once;

    fn _get_scale() -> &'static [f64] {
        static mut SCALE: Option<Vec<f64>> = None;
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let mut scale = Vec::with_capacity(2400);
            for i in 0..1000 {
                scale.push((i + 1) as f64);
            }

            for i in 0..900 {
                scale.push((1000 + 10 * (i + 1)) as f64);
            }

            for i in 0..500 {
                scale.push((10000 + 100 * (i + 1)) as f64);
            }
            unsafe { SCALE = Some(scale) }
        });

        unsafe { SCALE.as_ref().unwrap().as_slice() }
    }

    fn get_scale2() -> &'static [f64] {
        static mut SCALE: Option<Vec<f64>> = None;
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let mut scale = Vec::with_capacity(360);
            for i in 0..50 {
                scale.push((i + 1) as f64);
                print!("{},", i + 1);
            }
            println!("\n{}", scale.len());

            for i in (50..100).step_by(2) {
                scale.push((i + 2) as f64);
                print!("{},", i + 2);
            }
            println!("\n{}", scale.len());

            for i in (100..1000).step_by(10) {
                scale.push((i + 10) as f64);
                print!("{},", i + 10);
            }
            println!("\n{}", scale.len());

            for i in (1000..10000).step_by(100) {
                scale.push((i + 100) as f64);
                print!("{},", i + 100);
            }
            println!("\n{}", scale.len());

            for i in (10000..100000).step_by(1000) {
                scale.push((i + 1000) as f64);
                print!("{},", i + 1000);
            }
            println!("\n{}", scale.len());

            unsafe { SCALE = Some(scale) }
        });

        unsafe { SCALE.as_ref().unwrap().as_slice() }
    }

    #[test]
    pub fn percentile_test() {
        let scale = get_scale2();
        println!("scale length: {}", scale.len());

        let mut count_container = Vec::with_capacity(get_percentile_capacity(scale));
        for _ in 0..count_container.capacity() {
            count_container.push(0);
        }

        let mut percentile = Percentile::new(scale, count_container.as_mut_slice());

        for i in 0..3 {
            percentile.accumulate(i as f64);
        }

        let pos = percentile.get_result(95);
        println!("percentile value: {}", pos);
    }
}
