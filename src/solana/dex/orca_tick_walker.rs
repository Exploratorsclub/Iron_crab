// Copyright 2026 IronCrab contributors
// SPDX-License-Identifier: Apache-2.0
//
// Whirlpool CLMM swap math in `whirlpool_math` is ported from Orca Whirlpools
// (https://github.com/orca-so/whirlpools), programs/whirlpool/src/math/* —
// Apache-2.0. Minor adaptations: internal error enum, no on-chain logging.

use std::collections::HashMap;

use solana_sdk::pubkey::Pubkey;

use super::orca_tick_array::{
    swap_direction_tick_array_starts, ParsedTick, ParsedTickArray, TICK_ARRAY_SIZE,
};

#[derive(Debug, Clone)]
pub struct OrcaWhirlpoolQuoteInput {
    pub pool: Pubkey,
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub sqrt_price: u128,
    pub liquidity: u128,
    pub tick_current_index: i32,
    pub tick_spacing: u16,
    pub fee_rate: u16,
}

pub type OrcaTickArrays = HashMap<i32, ParsedTickArray>;

mod whirlpool_math {
    #![allow(dead_code, unused_imports)]

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum MathError {
        DivideByZero,
        MulDivOverflow,
        MultiplicationOverflow,
        MultiplicationShiftRightOverflow,
        TokenMaxExceeded,
        TokenMinSubceeded,
        SqrtPriceOutOfBounds,
        NumberDownCastError,
        LiquidityOverflow,
        LiquidityUnderflow,
    }

    pub mod u256 {
        use super::MathError;
        use std::{
            cmp::Ordering,
            fmt::{Display, Formatter, Result as FmtResult},
            str::from_utf8_unchecked,
        };

        const NUM_WORDS: usize = 4;

        #[derive(Copy, Clone, Debug)]
        pub struct U256Muldiv {
            pub items: [u64; NUM_WORDS],
        }

        impl U256Muldiv {
            pub fn new(h: u128, l: u128) -> Self {
                U256Muldiv {
                    items: [l.lo(), l.hi(), h.lo(), h.hi()],
                }
            }

            fn copy(&self) -> Self {
                let mut items: [u64; NUM_WORDS] = [0; NUM_WORDS];
                items.copy_from_slice(&self.items);
                U256Muldiv { items }
            }

            fn update_word(&mut self, index: usize, value: u64) {
                self.items[index] = value;
            }

            fn num_words(&self) -> usize {
                for i in (0..self.items.len()).rev() {
                    if self.items[i] != 0 {
                        return i + 1;
                    }
                }
                0
            }

            pub fn get_word(&self, index: usize) -> u64 {
                self.items[index]
            }

            pub fn get_word_u128(&self, index: usize) -> u128 {
                self.items[index] as u128
            }

            // Logical-left shift, does not trigger overflow
            pub fn shift_word_left(&self) -> Self {
                let mut result = U256Muldiv::new(0, 0);

                for i in (0..NUM_WORDS - 1).rev() {
                    result.items[i + 1] = self.items[i];
                }

                result
            }

            pub fn checked_shift_word_left(&self) -> Option<Self> {
                let last_element = self.items.last();

                match last_element {
                    None => Some(self.shift_word_left()),
                    Some(element) => {
                        if *element > 0 {
                            None
                        } else {
                            Some(self.shift_word_left())
                        }
                    }
                }
            }

            // Logical-left shift, does not trigger overflow
            pub fn shift_left(&self, mut shift_amount: u32) -> Self {
                // Return 0 if shift is greater than number of bits
                if shift_amount >= U64_RESOLUTION * (NUM_WORDS as u32) {
                    return U256Muldiv::new(0, 0);
                }

                let mut result = self.copy();

                while shift_amount >= U64_RESOLUTION {
                    result = result.shift_word_left();
                    shift_amount -= U64_RESOLUTION;
                }

                if shift_amount == 0 {
                    return result;
                }

                for i in (1..NUM_WORDS).rev() {
                    result.items[i] = (result.items[i] << shift_amount)
                        | (result.items[i - 1] >> (U64_RESOLUTION - shift_amount));
                }

                result.items[0] <<= shift_amount;

                result
            }

            // Logical-right shift, does not trigger overflow
            pub fn shift_word_right(&self) -> Self {
                let mut result = U256Muldiv::new(0, 0);

                for i in 0..NUM_WORDS - 1 {
                    result.items[i] = self.items[i + 1]
                }

                result
            }

            // Logical-right shift, does not trigger overflow
            pub fn shift_right(&self, mut shift_amount: u32) -> Self {
                // Return 0 if shift is greater than number of bits
                if shift_amount >= U64_RESOLUTION * (NUM_WORDS as u32) {
                    return U256Muldiv::new(0, 0);
                }

                let mut result = self.copy();

                while shift_amount >= U64_RESOLUTION {
                    result = result.shift_word_right();
                    shift_amount -= U64_RESOLUTION;
                }

                if shift_amount == 0 {
                    return result;
                }

                for i in 0..NUM_WORDS - 1 {
                    result.items[i] = (result.items[i] >> shift_amount)
                        | (result.items[i + 1] << (U64_RESOLUTION - shift_amount));
                }

                result.items[3] >>= shift_amount;

                result
            }

            #[allow(clippy::should_implement_trait)]
            pub fn eq(&self, other: U256Muldiv) -> bool {
                for i in 0..self.items.len() {
                    if self.items[i] != other.items[i] {
                        return false;
                    }
                }

                true
            }

            pub fn lt(&self, other: U256Muldiv) -> bool {
                for i in (0..self.items.len()).rev() {
                    match self.items[i].cmp(&other.items[i]) {
                        Ordering::Less => return true,
                        Ordering::Greater => return false,
                        Ordering::Equal => {}
                    }
                }

                false
            }

            pub fn gt(&self, other: U256Muldiv) -> bool {
                for i in (0..self.items.len()).rev() {
                    match self.items[i].cmp(&other.items[i]) {
                        Ordering::Less => return false,
                        Ordering::Greater => return true,
                        Ordering::Equal => {}
                    }
                }

                false
            }

            pub fn lte(&self, other: U256Muldiv) -> bool {
                for i in (0..self.items.len()).rev() {
                    match self.items[i].cmp(&other.items[i]) {
                        Ordering::Less => return true,
                        Ordering::Greater => return false,
                        Ordering::Equal => {}
                    }
                }

                true
            }

            pub fn gte(&self, other: U256Muldiv) -> bool {
                for i in (0..self.items.len()).rev() {
                    match self.items[i].cmp(&other.items[i]) {
                        Ordering::Less => return false,
                        Ordering::Greater => return true,
                        Ordering::Equal => {}
                    }
                }

                true
            }

            pub fn try_into_u128(&self) -> Result<u128, MathError> {
                if self.num_words() > 2 {
                    return Err(MathError::NumberDownCastError);
                }

                Ok(((self.items[1] as u128) << U64_RESOLUTION) | (self.items[0] as u128))
            }

            pub fn is_zero(self) -> bool {
                for i in 0..NUM_WORDS {
                    if self.items[i] != 0 {
                        return false;
                    }
                }

                true
            }

            // Input:
            //  m = U256::MAX + 1 (which is the amount used for overflow)
            //  n = input value
            // Output:
            //  r = smallest positive additive inverse of n mod m
            //
            // We wish to find r, s.t., r + n ≡ 0 mod m;
            // We generally wish to find this r since r ≡ -n mod m
            // and can make operations with n with large number of bits
            // fit into u256 space without overflow
            pub fn get_add_inverse(&self) -> Self {
                // Additive inverse of 0 is 0
                if self.eq(U256Muldiv::new(0, 0)) {
                    return U256Muldiv::new(0, 0);
                }
                // To ensure we don't overflow, we begin with max and do a subtraction
                U256Muldiv::new(u128::MAX, u128::MAX)
                    .sub(*self)
                    .add(U256Muldiv::new(0, 1))
            }

            // Result overflows if the result is greater than 2^256-1
            pub fn add(&self, other: U256Muldiv) -> Self {
                let mut result = U256Muldiv::new(0, 0);

                let mut carry = 0;
                for i in 0..NUM_WORDS {
                    let x = self.get_word_u128(i);
                    let y = other.get_word_u128(i);
                    let t = x + y + carry;
                    result.update_word(i, t.lo());

                    carry = t.hi_u128();
                }

                result
            }

            // Result underflows if the result is greater than 2^256-1
            pub fn sub(&self, other: U256Muldiv) -> Self {
                let mut result = U256Muldiv::new(0, 0);

                let mut carry = 0;
                for i in 0..NUM_WORDS {
                    let x = self.get_word(i);
                    let y = other.get_word(i);
                    let (t0, overflowing0) = x.overflowing_sub(y);
                    let (t1, overflowing1) = t0.overflowing_sub(carry);
                    result.update_word(i, t1);

                    carry = if overflowing0 || overflowing1 { 1 } else { 0 };
                }

                result
            }

            // Result overflows if great than 2^256-1
            pub fn mul(&self, other: U256Muldiv) -> Self {
                let mut result = U256Muldiv::new(0, 0);

                let m = self.num_words();
                let n = other.num_words();

                for j in 0..n {
                    let mut k = 0;
                    for i in 0..m {
                        let x = self.get_word_u128(i);
                        let y = other.get_word_u128(j);
                        if i + j < NUM_WORDS {
                            let z = result.get_word_u128(i + j);
                            let t = x.wrapping_mul(y).wrapping_add(z).wrapping_add(k);
                            result.update_word(i + j, t.lo());
                            k = t.hi_u128();
                        }
                    }

                    // Don't update the carry word
                    if j + m < NUM_WORDS {
                        result.update_word(j + m, k as u64);
                    }
                }

                result
            }

            // Result returns 0 if divide by zero
            pub fn div(&self, mut divisor: U256Muldiv, return_remainder: bool) -> (Self, Self) {
                let mut dividend = self.copy();
                let mut quotient = U256Muldiv::new(0, 0);

                let num_dividend_words = dividend.num_words();
                let num_divisor_words = divisor.num_words();

                if num_divisor_words == 0 {
                    panic!("divide by zero");
                }

                // Case 0. If either the dividend or divisor is 0, return 0
                if num_dividend_words == 0 {
                    return (U256Muldiv::new(0, 0), U256Muldiv::new(0, 0));
                }

                // Case 1. Dividend is smaller than divisor, quotient = 0, remainder = dividend
                if num_dividend_words < num_divisor_words {
                    if return_remainder {
                        return (U256Muldiv::new(0, 0), dividend);
                    } else {
                        return (U256Muldiv::new(0, 0), U256Muldiv::new(0, 0));
                    }
                }

                // Case 2. Dividend is smaller than u128, divisor <= dividend, perform math in u128 space
                if num_dividend_words < 3 {
                    let dividend = dividend.try_into_u128().unwrap();
                    let divisor = divisor.try_into_u128().unwrap();
                    let quotient = dividend / divisor;
                    if return_remainder {
                        let remainder = dividend % divisor;
                        return (U256Muldiv::new(0, quotient), U256Muldiv::new(0, remainder));
                    } else {
                        return (U256Muldiv::new(0, quotient), U256Muldiv::new(0, 0));
                    }
                }

                // Case 3. Divisor is single-word, we must isolate this case for correctness
                if num_divisor_words == 1 {
                    let mut k = 0;
                    for j in (0..num_dividend_words).rev() {
                        let d1 = hi_lo(k.lo(), dividend.get_word(j));
                        let d2 = divisor.get_word_u128(0);
                        let q = d1 / d2;
                        k = d1 - d2 * q;
                        quotient.update_word(j, q.lo());
                    }

                    if return_remainder {
                        return (quotient, U256Muldiv::new(0, k));
                    } else {
                        return (quotient, U256Muldiv::new(0, 0));
                    }
                }

                // Normalize the division by shifting left
                let s = divisor.get_word(num_divisor_words - 1).leading_zeros();
                let b = dividend.get_word(num_dividend_words - 1).leading_zeros();

                // Conditional carry space for normalized division
                let mut dividend_carry_space: u64 = 0;
                if num_dividend_words == NUM_WORDS && b < s {
                    dividend_carry_space =
                        dividend.items[num_dividend_words - 1] >> (U64_RESOLUTION - s);
                }
                dividend = dividend.shift_left(s);
                divisor = divisor.shift_left(s);

                for j in (0..num_dividend_words - num_divisor_words + 1).rev() {
                    let result = div_loop(
                        j,
                        num_divisor_words,
                        dividend,
                        &mut dividend_carry_space,
                        divisor,
                        quotient,
                    );
                    quotient = result.0;
                    dividend = result.1;
                }

                if return_remainder {
                    dividend = dividend.shift_right(s);
                    (quotient, dividend)
                } else {
                    (quotient, U256Muldiv::new(0, 0))
                }
            }
        }

        impl Display for U256Muldiv {
            fn fmt(&self, f: &mut Formatter) -> FmtResult {
                let mut buf = [0_u8; NUM_WORDS * 20];
                let mut i = buf.len() - 1;

                let ten = U256Muldiv::new(0, 10);
                let mut current = *self;

                loop {
                    let (quotient, remainder) = current.div(ten, true);
                    let digit = remainder.get_word(0) as u8;
                    buf[i] = digit + b'0';
                    current = quotient;

                    if current.is_zero() {
                        break;
                    }

                    i -= 1;
                }

                let s = unsafe { from_utf8_unchecked(&buf[i..]) };

                f.write_str(s)
            }
        }

        impl From<u128> for U256Muldiv {
            fn from(value: u128) -> Self {
                // A u128 value only occupies the low 128 bits (l) of the U256Muldiv.
                // The high 128 bits (h) are set to 0.
                U256Muldiv::new(0, value)
            }
        }

        impl From<u64> for U256Muldiv {
            fn from(value: u64) -> Self {
                // A u64 value only occupies the low 64 bits of the lower 128 bits (l) of the U256Muldiv.
                // The high 64 bits of the lower 128 bits (l) are set to 0 by type casting.
                // The high 128 bits (h) are set to 0 below.
                U256Muldiv::new(0, value as u128)
            }
        }

        const U64_MAX: u128 = u64::MAX as u128;
        const U64_RESOLUTION: u32 = 64;

        pub trait LoHi {
            fn lo(self) -> u64;
            fn hi(self) -> u64;
            fn lo_u128(self) -> u128;
            fn hi_u128(self) -> u128;
        }

        impl LoHi for u128 {
            fn lo(self) -> u64 {
                (self & U64_MAX) as u64
            }
            fn lo_u128(self) -> u128 {
                self & U64_MAX
            }
            fn hi(self) -> u64 {
                (self >> U64_RESOLUTION) as u64
            }
            fn hi_u128(self) -> u128 {
                self >> U64_RESOLUTION
            }
        }

        pub fn hi_lo(hi: u64, lo: u64) -> u128 {
            ((hi as u128) << U64_RESOLUTION) | (lo as u128)
        }

        pub fn mul_u256(v: u128, n: u128) -> U256Muldiv {
            // do 128 bits multiply
            //                   nh   nl
            //                *  vh   vl
            //                ----------
            // a0 =              vl * nl
            // a1 =         vl * nh
            // b0 =         vh * nl
            // b1 =  + vh * nh
            //       -------------------
            //        c1h  c1l  c0h  c0l
            //
            // "a0" is optimized away, result is stored directly in c0.  "b1" is
            // optimized away, result is stored directly in c1.
            //

            let mut c0 = v.lo_u128() * n.lo_u128();
            let a1 = v.lo_u128() * n.hi_u128();
            let b0 = v.hi_u128() * n.lo_u128();

            // add the high word of a0 to the low words of a1 and b0 using c1 as
            // scrach space to capture the carry.  the low word of the result becomes
            // the final high word of c0
            let mut c1 = c0.hi_u128() + a1.lo_u128() + b0.lo_u128();

            c0 = hi_lo(c1.lo(), c0.lo());

            // add the carry from the result above (found in the high word of c1) and
            // the high words of a1 and b0 to b1, the result is c1.
            c1 = v.hi_u128() * n.hi_u128() + c1.hi_u128() + a1.hi_u128() + b0.hi_u128();

            U256Muldiv::new(c1, c0)
        }

        fn div_loop(
            index: usize,
            num_divisor_words: usize,
            mut dividend: U256Muldiv,
            dividend_carry_space: &mut u64,
            divisor: U256Muldiv,
            mut quotient: U256Muldiv,
        ) -> (U256Muldiv, U256Muldiv) {
            let use_carry = (index + num_divisor_words) == NUM_WORDS;
            let div_hi = if use_carry {
                *dividend_carry_space
            } else {
                dividend.get_word(index + num_divisor_words)
            };
            let d0 = hi_lo(div_hi, dividend.get_word(index + num_divisor_words - 1));
            let d1 = divisor.get_word_u128(num_divisor_words - 1);

            let mut qhat = d0 / d1;
            let mut rhat = d0 - d1 * qhat;

            let d0_2 = dividend.get_word(index + num_divisor_words - 2);
            let d1_2 = divisor.get_word_u128(num_divisor_words - 2);

            let mut cmp1 = hi_lo(rhat.lo(), d0_2);
            let mut cmp2 = qhat.wrapping_mul(d1_2);

            while qhat.hi() != 0 || cmp2 > cmp1 {
                qhat -= 1;
                rhat += d1;
                if rhat.hi() != 0 {
                    break;
                }

                cmp1 = hi_lo(rhat.lo(), cmp1.lo());
                cmp2 -= d1_2;
            }

            let mut k = 0;
            let mut t;
            for i in 0..num_divisor_words {
                let p = qhat * (divisor.get_word_u128(i));
                t = (dividend.get_word_u128(index + i))
                    .wrapping_sub(k)
                    .wrapping_sub(p.lo_u128());
                dividend.update_word(index + i, t.lo());
                k = ((p >> U64_RESOLUTION) as u64).wrapping_sub((t >> U64_RESOLUTION) as u64)
                    as u128;
            }

            let d_head = if use_carry {
                *dividend_carry_space as u128
            } else {
                dividend.get_word_u128(index + num_divisor_words)
            };

            t = d_head.wrapping_sub(k);
            if use_carry {
                *dividend_carry_space = t.lo();
            } else {
                dividend.update_word(index + num_divisor_words, t.lo());
            }

            if k > d_head {
                qhat -= 1;
                k = 0;
                for i in 0..num_divisor_words {
                    t = dividend
                        .get_word_u128(index + i)
                        .wrapping_add(divisor.get_word_u128(i))
                        .wrapping_add(k);
                    dividend.update_word(index + i, t.lo());
                    k = t >> U64_RESOLUTION;
                }

                let new_carry = dividend
                    .get_word_u128(index + num_divisor_words)
                    .wrapping_add(k)
                    .lo();
                if use_carry {
                    *dividend_carry_space = new_carry
                } else {
                    dividend.update_word(
                        index + num_divisor_words,
                        dividend
                            .get_word_u128(index + num_divisor_words)
                            .wrapping_add(k)
                            .lo(),
                    );
                }
            }

            quotient.update_word(index, qhat.lo());

            (quotient, dividend)
        }
    }

    pub mod bit {
        use super::u256::U256Muldiv;
        use super::MathError;

        pub const Q64_RESOLUTION: u8 = 64;
        pub const Q64_MASK: u128 = 0xFFFF_FFFF_FFFF_FFFF;
        pub const TO_Q64: u128 = 1u128 << Q64_RESOLUTION;

        pub fn checked_mul_div(n0: u128, n1: u128, d: u128) -> Result<u128, MathError> {
            checked_mul_div_round_up_if(n0, n1, d, false)
        }

        pub fn checked_mul_div_round_up(n0: u128, n1: u128, d: u128) -> Result<u128, MathError> {
            checked_mul_div_round_up_if(n0, n1, d, true)
        }

        pub fn checked_mul_div_round_up_if(
            n0: u128,
            n1: u128,
            d: u128,
            round_up: bool,
        ) -> Result<u128, MathError> {
            if d == 0 {
                return Err(MathError::DivideByZero);
            }

            let p = n0.checked_mul(n1).ok_or(MathError::MulDivOverflow)?;
            let n = p / d;

            Ok(if round_up && p % d > 0 { n + 1 } else { n })
        }

        pub fn checked_mul_shift_right(n0: u128, n1: u128) -> Result<u64, MathError> {
            checked_mul_shift_right_round_up_if(n0, n1, false)
        }

        /// Multiplies an integer u128 and a Q64.64 fixed point number.
        /// Returns a product represented as a u64 integer.
        pub fn checked_mul_shift_right_round_up_if(
            n0: u128,
            n1: u128,
            round_up: bool,
        ) -> Result<u64, MathError> {
            // customized this function is used in try_get_amount_delta_b (token_math.rs)

            if n0 == 0 || n1 == 0 {
                return Ok(0);
            }

            let p = n0
                .checked_mul(n1)
                .ok_or(MathError::MultiplicationShiftRightOverflow)?;

            let result = (p >> Q64_RESOLUTION) as u64;

            let should_round = round_up && (p & Q64_MASK > 0);
            if should_round && result == u64::MAX {
                return Err(MathError::MultiplicationOverflow);
            }

            Ok(if should_round { result + 1 } else { result })
        }

        pub fn div_round_up(n: u128, d: u128) -> Result<u128, MathError> {
            div_round_up_if(n, d, true)
        }

        pub fn div_round_up_if(n: u128, d: u128, round_up: bool) -> Result<u128, MathError> {
            if d == 0 {
                return Err(MathError::DivideByZero);
            }

            let q = n / d;

            Ok(if round_up && n % d > 0 { q + 1 } else { q })
        }

        pub fn div_round_up_if_u256(
            n: U256Muldiv,
            d: U256Muldiv,
            round_up: bool,
        ) -> Result<u128, MathError> {
            let (quotient, remainder) = n.div(d, round_up);

            let result = if round_up && !remainder.is_zero() {
                quotient.add(U256Muldiv::new(0, 1))
            } else {
                quotient
            };

            result.try_into_u128()
        }
    }

    pub mod tick {
        use super::u256::{mul_u256, U256Muldiv};

        // Max/Min sqrt_price derived from max/min tick-index
        pub const MAX_SQRT_PRICE_X64: u128 = 79226673515401279992447579055;
        pub const MIN_SQRT_PRICE_X64: u128 = 4295048016;

        const LOG_B_2_X32: i128 = 59543866431248i128;
        const BIT_PRECISION: u32 = 14;
        const LOG_B_P_ERR_MARGIN_LOWER_X64: i128 = 184467440737095516i128; // 0.01
        const LOG_B_P_ERR_MARGIN_UPPER_X64: i128 = 15793534762490258745i128; // 2^-precision / log_2_b + 0.01

        pub const FULL_RANGE_ONLY_TICK_SPACING_THRESHOLD: u16 = 32768; // 2^15

        /// Derive the sqrt-price from a tick index. The precision of this method is only guarranted
        /// if tick is within the bounds of {max, min} tick-index.
        ///
        /// # Parameters
        /// - `tick` - A i32 integer representing the tick integer
        ///
        /// # Returns
        /// - `Ok`: A u128 Q32.64 representing the sqrt_price
        pub fn sqrt_price_from_tick_index(tick: i32) -> u128 {
            if tick >= 0 {
                get_sqrt_price_positive_tick(tick)
            } else {
                get_sqrt_price_negative_tick(tick)
            }
        }

        /// Derive the tick-index from a sqrt-price. The precision of this method is only guarranted
        /// if sqrt-price is within the bounds of {max, min} sqrt-price.
        ///
        /// # Parameters
        /// - `sqrt_price_x64` - A u128 Q64.64 integer representing the sqrt-price
        ///
        /// # Returns
        /// - An i32 representing the tick_index of the provided sqrt-price
        fn mul_shift_96(n0: u128, n1: u128) -> u128 {
            mul_u256(n0, n1).shift_right(96).try_into_u128().unwrap()
        }

        // Performs the exponential conversion with Q64.64 precision
        fn get_sqrt_price_positive_tick(tick: i32) -> u128 {
            let mut ratio: u128 = if tick & 1 != 0 {
                79232123823359799118286999567
            } else {
                79228162514264337593543950336
            };

            if tick & 2 != 0 {
                ratio = mul_shift_96(ratio, 79236085330515764027303304731);
            }
            if tick & 4 != 0 {
                ratio = mul_shift_96(ratio, 79244008939048815603706035061);
            }
            if tick & 8 != 0 {
                ratio = mul_shift_96(ratio, 79259858533276714757314932305);
            }
            if tick & 16 != 0 {
                ratio = mul_shift_96(ratio, 79291567232598584799939703904);
            }
            if tick & 32 != 0 {
                ratio = mul_shift_96(ratio, 79355022692464371645785046466);
            }
            if tick & 64 != 0 {
                ratio = mul_shift_96(ratio, 79482085999252804386437311141);
            }
            if tick & 128 != 0 {
                ratio = mul_shift_96(ratio, 79736823300114093921829183326);
            }
            if tick & 256 != 0 {
                ratio = mul_shift_96(ratio, 80248749790819932309965073892);
            }
            if tick & 512 != 0 {
                ratio = mul_shift_96(ratio, 81282483887344747381513967011);
            }
            if tick & 1024 != 0 {
                ratio = mul_shift_96(ratio, 83390072131320151908154831281);
            }
            if tick & 2048 != 0 {
                ratio = mul_shift_96(ratio, 87770609709833776024991924138);
            }
            if tick & 4096 != 0 {
                ratio = mul_shift_96(ratio, 97234110755111693312479820773);
            }
            if tick & 8192 != 0 {
                ratio = mul_shift_96(ratio, 119332217159966728226237229890);
            }
            if tick & 16384 != 0 {
                ratio = mul_shift_96(ratio, 179736315981702064433883588727);
            }
            if tick & 32768 != 0 {
                ratio = mul_shift_96(ratio, 407748233172238350107850275304);
            }
            if tick & 65536 != 0 {
                ratio = mul_shift_96(ratio, 2098478828474011932436660412517);
            }
            if tick & 131072 != 0 {
                ratio = mul_shift_96(ratio, 55581415166113811149459800483533);
            }
            if tick & 262144 != 0 {
                ratio = mul_shift_96(ratio, 38992368544603139932233054999993551);
            }

            ratio >> 32
        }

        fn get_sqrt_price_negative_tick(tick: i32) -> u128 {
            let abs_tick = tick.abs();

            let mut ratio: u128 = if abs_tick & 1 != 0 {
                18445821805675392311
            } else {
                18446744073709551616
            };

            if abs_tick & 2 != 0 {
                ratio = (ratio * 18444899583751176498) >> 64
            }
            if abs_tick & 4 != 0 {
                ratio = (ratio * 18443055278223354162) >> 64
            }
            if abs_tick & 8 != 0 {
                ratio = (ratio * 18439367220385604838) >> 64
            }
            if abs_tick & 16 != 0 {
                ratio = (ratio * 18431993317065449817) >> 64
            }
            if abs_tick & 32 != 0 {
                ratio = (ratio * 18417254355718160513) >> 64
            }
            if abs_tick & 64 != 0 {
                ratio = (ratio * 18387811781193591352) >> 64
            }
            if abs_tick & 128 != 0 {
                ratio = (ratio * 18329067761203520168) >> 64
            }
            if abs_tick & 256 != 0 {
                ratio = (ratio * 18212142134806087854) >> 64
            }
            if abs_tick & 512 != 0 {
                ratio = (ratio * 17980523815641551639) >> 64
            }
            if abs_tick & 1024 != 0 {
                ratio = (ratio * 17526086738831147013) >> 64
            }
            if abs_tick & 2048 != 0 {
                ratio = (ratio * 16651378430235024244) >> 64
            }
            if abs_tick & 4096 != 0 {
                ratio = (ratio * 15030750278693429944) >> 64
            }
            if abs_tick & 8192 != 0 {
                ratio = (ratio * 12247334978882834399) >> 64
            }
            if abs_tick & 16384 != 0 {
                ratio = (ratio * 8131365268884726200) >> 64
            }
            if abs_tick & 32768 != 0 {
                ratio = (ratio * 3584323654723342297) >> 64
            }
            if abs_tick & 65536 != 0 {
                ratio = (ratio * 696457651847595233) >> 64
            }
            if abs_tick & 131072 != 0 {
                ratio = (ratio * 26294789957452057) >> 64
            }
            if abs_tick & 262144 != 0 {
                ratio = (ratio * 37481735321082) >> 64
            }

            ratio
        }
    }

    pub mod token {
        use super::bit::{div_round_up_if, div_round_up_if_u256, Q64_MASK, Q64_RESOLUTION};
        use super::tick::{sqrt_price_from_tick_index, MAX_SQRT_PRICE_X64, MIN_SQRT_PRICE_X64};
        use super::u256::{mul_u256, U256Muldiv};
        use super::MathError;

        // Fee rate is represented as hundredths of a basis point.
        // Fee amount = total_amount * fee_rate / 1_000_000.
        // Max fee rate supported is 6%.
        pub const MAX_FEE_RATE: u16 = 60_000;

        // Assuming that FEE_RATE is represented as hundredths of a basis point
        // We want FEE_RATE_MUL_VALUE = 1/FEE_RATE_UNIT, so 1e6
        pub const FEE_RATE_MUL_VALUE: u128 = 1_000_000;

        // Protocol fee rate is represented as a basis point.
        // Protocol fee amount = fee_amount * protocol_fee_rate / 10_000.
        // Max protocol fee rate supported is 25% of the fee rate.
        pub const MAX_PROTOCOL_FEE_RATE: u16 = 2_500;

        // Assuming that PROTOCOL_FEE_RATE is represented as a basis point
        // We want PROTOCOL_FEE_RATE_MUL_VALUE = 1/PROTOCOL_FEE_UNIT, so 1e4
        pub const PROTOCOL_FEE_RATE_MUL_VALUE: u128 = 10_000;

        #[derive(Debug)]
        pub enum AmountDeltaU64 {
            Valid(u64),
            ExceedsMax(MathError),
        }

        impl AmountDeltaU64 {
            pub fn lte(&self, other: u64) -> bool {
                match self {
                    AmountDeltaU64::Valid(value) => *value <= other,
                    AmountDeltaU64::ExceedsMax(_) => false,
                }
            }

            pub fn exceeds_max(&self) -> bool {
                match self {
                    AmountDeltaU64::Valid(_) => false,
                    AmountDeltaU64::ExceedsMax(_) => true,
                }
            }

            pub fn value(self) -> u64 {
                match self {
                    AmountDeltaU64::Valid(value) => value,
                    // This should never happen
                    AmountDeltaU64::ExceedsMax(_) => {
                        panic!("Called unwrap on AmountDeltaU64::ExceedsMax")
                    }
                }
            }
        }

        //
        // Get change in token_a corresponding to a change in price
        //

        // 6.16
        // Δt_a = Δ(1 / sqrt_price) * liquidity

        // Replace delta
        // Δt_a = (1 / sqrt_price_upper - 1 / sqrt_price_lower) * liquidity

        // Common denominator to simplify
        // Δt_a = ((sqrt_price_lower - sqrt_price_upper) / (sqrt_price_upper * sqrt_price_lower)) * liquidity

        // Δt_a = (liquidity * (sqrt_price_lower - sqrt_price_upper)) / (sqrt_price_upper * sqrt_price_lower)
        pub fn get_amount_delta_a(
            sqrt_price_0: u128,
            sqrt_price_1: u128,
            liquidity: u128,
            round_up: bool,
        ) -> Result<u64, MathError> {
            match try_get_amount_delta_a(sqrt_price_0, sqrt_price_1, liquidity, round_up) {
                Ok(AmountDeltaU64::Valid(value)) => Ok(value),
                Ok(AmountDeltaU64::ExceedsMax(error)) => Err(error),
                Err(error) => Err(error),
            }
        }

        pub fn try_get_amount_delta_a(
            sqrt_price_0: u128,
            sqrt_price_1: u128,
            liquidity: u128,
            round_up: bool,
        ) -> Result<AmountDeltaU64, MathError> {
            let (sqrt_price_lower, sqrt_price_upper) =
                increasing_price_order(sqrt_price_0, sqrt_price_1);

            let sqrt_price_diff = sqrt_price_upper - sqrt_price_lower;

            let numerator = mul_u256(liquidity, sqrt_price_diff)
                .checked_shift_word_left()
                .ok_or(MathError::MultiplicationOverflow)?;

            let denominator = mul_u256(sqrt_price_upper, sqrt_price_lower);

            let (quotient, remainder) = numerator.div(denominator, round_up);

            let result = if round_up && !remainder.is_zero() {
                quotient.add(U256Muldiv::new(0, 1)).try_into_u128()
            } else {
                quotient.try_into_u128()
            };

            match result {
                Ok(result) => {
                    if result > u64::MAX as u128 {
                        return Ok(AmountDeltaU64::ExceedsMax(MathError::TokenMaxExceeded));
                    }

                    Ok(AmountDeltaU64::Valid(result as u64))
                }
                Err(err) => Ok(AmountDeltaU64::ExceedsMax(err)),
            }
        }

        //
        // Get change in token_b corresponding to a change in price
        //

        // 6.14
        // Δt_b = Δ(sqrt_price) * liquidity

        // Replace delta
        // Δt_b = (sqrt_price_upper - sqrt_price_lower) * liquidity
        pub fn get_amount_delta_b(
            sqrt_price_0: u128,
            sqrt_price_1: u128,
            liquidity: u128,
            round_up: bool,
        ) -> Result<u64, MathError> {
            match try_get_amount_delta_b(sqrt_price_0, sqrt_price_1, liquidity, round_up) {
                Ok(AmountDeltaU64::Valid(value)) => Ok(value),
                Ok(AmountDeltaU64::ExceedsMax(error)) => Err(error),
                Err(error) => Err(error),
            }
        }

        pub fn try_get_amount_delta_b(
            sqrt_price_0: u128,
            sqrt_price_1: u128,
            liquidity: u128,
            round_up: bool,
        ) -> Result<AmountDeltaU64, MathError> {
            let (sqrt_price_lower, sqrt_price_upper) =
                increasing_price_order(sqrt_price_0, sqrt_price_1);

            // customized checked_mul_shift_right_round_up_if

            let n0 = liquidity;
            let n1 = sqrt_price_upper - sqrt_price_lower;

            if n0 == 0 || n1 == 0 {
                return Ok(AmountDeltaU64::Valid(0));
            }

            if let Some(p) = n0.checked_mul(n1) {
                let result = (p >> Q64_RESOLUTION) as u64;

                let should_round = round_up && (p & Q64_MASK > 0);
                if should_round && result == u64::MAX {
                    return Ok(AmountDeltaU64::ExceedsMax(
                        MathError::MultiplicationOverflow,
                    ));
                }

                Ok(AmountDeltaU64::Valid(if should_round {
                    result + 1
                } else {
                    result
                }))
            } else {
                Ok(AmountDeltaU64::ExceedsMax(
                    MathError::MultiplicationShiftRightOverflow,
                ))
            }
        }

        pub fn increasing_price_order(sqrt_price_0: u128, sqrt_price_1: u128) -> (u128, u128) {
            if sqrt_price_0 > sqrt_price_1 {
                (sqrt_price_1, sqrt_price_0)
            } else {
                (sqrt_price_0, sqrt_price_1)
            }
        }

        //
        // Get change in price corresponding to a change in token_a supply
        //
        // 6.15
        // Δ(1 / sqrt_price) = Δt_a / liquidity
        //
        // Replace delta
        // 1 / sqrt_price_new - 1 / sqrt_price = amount / liquidity
        //
        // Move sqrt price to other side
        // 1 / sqrt_price_new = (amount / liquidity) + (1 / sqrt_price)
        //
        // Common denominator for right side
        // 1 / sqrt_price_new = (sqrt_price * amount + liquidity) / (sqrt_price * liquidity)
        //
        // Invert fractions
        // sqrt_price_new = (sqrt_price * liquidity) / (liquidity + amount * sqrt_price)
        pub fn get_next_sqrt_price_from_a_round_up(
            sqrt_price: u128,
            liquidity: u128,
            amount: u64,
            amount_specified_is_input: bool,
        ) -> Result<u128, MathError> {
            if amount == 0 {
                return Ok(sqrt_price);
            }
            let product = mul_u256(sqrt_price, amount as u128);

            let numerator = mul_u256(liquidity, sqrt_price)
                .checked_shift_word_left()
                .ok_or(MathError::MultiplicationOverflow)?;

            // In this scenario the denominator will end up being < 0
            let liquidity_shift_left = U256Muldiv::new(0, liquidity).shift_word_left();
            if !amount_specified_is_input && liquidity_shift_left.lte(product) {
                return Err(MathError::DivideByZero);
            }

            let denominator = if amount_specified_is_input {
                liquidity_shift_left.add(product)
            } else {
                liquidity_shift_left.sub(product)
            };

            let price = div_round_up_if_u256(numerator, denominator, true)?;
            if price < MIN_SQRT_PRICE_X64 {
                return Err(MathError::TokenMinSubceeded);
            } else if price > MAX_SQRT_PRICE_X64 {
                return Err(MathError::TokenMaxExceeded);
            }

            Ok(price)
        }

        //
        // Get change in price corresponding to a change in token_b supply
        //
        // 6.13
        // Δ(sqrt_price) = Δt_b / liquidity
        pub fn get_next_sqrt_price_from_b_round_down(
            sqrt_price: u128,
            liquidity: u128,
            amount: u64,
            amount_specified_is_input: bool,
        ) -> Result<u128, MathError> {
            // We always want square root price to be rounded down, which means
            // Case 3. If we are fixing input (adding B), we are increasing price, we want delta to be floor(delta)
            // sqrt_price + floor(delta) < sqrt_price + delta
            //
            // Case 4. If we are fixing output (removing B), we are decreasing price, we want delta to be ceil(delta)
            // sqrt_price - ceil(delta) < sqrt_price - delta

            // Q64.0 << 64 => Q64.64
            let amount_x64 = (amount as u128) << Q64_RESOLUTION;

            // Q64.64 / Q64.0 => Q64.64
            let delta = div_round_up_if(amount_x64, liquidity, !amount_specified_is_input)?;

            // Q64(32).64 +/- Q64.64
            if amount_specified_is_input {
                // We are adding token b to supply, causing price to increase
                sqrt_price
                    .checked_add(delta)
                    .ok_or(MathError::SqrtPriceOutOfBounds)
            } else {
                // We are removing token b from supply,. causing price to decrease
                sqrt_price
                    .checked_sub(delta)
                    .ok_or(MathError::SqrtPriceOutOfBounds)
            }
        }

        pub fn get_next_sqrt_price(
            sqrt_price: u128,
            liquidity: u128,
            amount: u64,
            amount_specified_is_input: bool,
            a_to_b: bool,
        ) -> Result<u128, MathError> {
            if amount_specified_is_input == a_to_b {
                // We are fixing A
                // Case 1. amount_specified_is_input = true, a_to_b = true
                // We are exchanging A to B with at most _amount_ of A (input)
                //
                // Case 2. amount_specified_is_input = false, a_to_b = false
                // We are exchanging B to A wanting to guarantee at least _amount_ of A (output)
                //
                // In either case we want the sqrt_price to be rounded up.
                //
                // Eq 1. sqrt_price = sqrt( b / a )
                //
                // Case 1. amount_specified_is_input = true, a_to_b = true
                // We are adding token A to the supply, causing price to decrease (Eq 1.)
                // Since we are fixing input, we can not exceed the amount that is being provided by the user.
                // Because a higher price is inversely correlated with an increased supply of A,
                // a higher price means we are adding less A. Thus when performing math, we wish to round the
                // price up, since that means that we are guaranteed to not exceed the fixed amount of A provided.
                //
                // Case 2. amount_specified_is_input = false, a_to_b = false
                // We are removing token A from the supply, causing price to increase (Eq 1.)
                // Since we are fixing output, we want to guarantee that the user is provided at least _amount_ of A
                // Because a higher price is correlated with a decreased supply of A,
                // a higher price means we are removing more A to give to the user. Thus when performing math, we wish
                // to round the price up, since that means we guarantee that user receives at least _amount_ of A
                get_next_sqrt_price_from_a_round_up(
                    sqrt_price,
                    liquidity,
                    amount,
                    amount_specified_is_input,
                )
            } else {
                // We are fixing B
                // Case 3. amount_specified_is_input = true, a_to_b = false
                // We are exchanging B to A using at most _amount_ of B (input)
                //
                // Case 4. amount_specified_is_input = false, a_to_b = true
                // We are exchanging A to B wanting to guarantee at least _amount_ of B (output)
                //
                // In either case we want the sqrt_price to be rounded down.
                //
                // Eq 1. sqrt_price = sqrt( b / a )
                //
                // Case 3. amount_specified_is_input = true, a_to_b = false
                // We are adding token B to the supply, causing price to increase (Eq 1.)
                // Since we are fixing input, we can not exceed the amount that is being provided by the user.
                // Because a lower price is inversely correlated with an increased supply of B,
                // a lower price means that we are adding less B. Thus when performing math, we wish to round the
                // price down, since that means that we are guaranteed to not exceed the fixed amount of B provided.
                //
                // Case 4. amount_specified_is_input = false, a_to_b = true
                // We are removing token B from the supply, causing price to decrease (Eq 1.)
                // Since we are fixing output, we want to guarantee that the user is provided at least _amount_ of B
                // Because a lower price is correlated with a decreased supply of B,
                // a lower price means we are removing more B to give to the user. Thus when performing math, we
                // wish to round the price down, since that means we guarantee that the user receives at least _amount_ of B
                get_next_sqrt_price_from_b_round_down(
                    sqrt_price,
                    liquidity,
                    amount,
                    amount_specified_is_input,
                )
            }
        }

        /// Estimate the maximum liquidity that can be added given token A/B maximums, current price,
        /// and position tick bounds. Liquidity is rounded down to prevent token inputs exceeding their maxima.
        pub fn estimate_max_liquidity_from_token_amounts(
            current_sqrt_price: u128,
            tick_lower_index: i32,
            tick_upper_index: i32,
            token_max_a: u64,
            token_max_b: u64,
        ) -> Result<u128, MathError> {
            let lower_sqrt_price = sqrt_price_from_tick_index(tick_lower_index);
            let upper_sqrt_price = sqrt_price_from_tick_index(tick_upper_index);

            if current_sqrt_price >= upper_sqrt_price {
                // Entirely above range – constrained by token B
                est_liquidity_for_token_b(upper_sqrt_price, lower_sqrt_price, token_max_b)
            } else if current_sqrt_price <= lower_sqrt_price {
                // Entirely below range – constrained by token A
                est_liquidity_for_token_a(lower_sqrt_price, upper_sqrt_price, token_max_a)
            } else {
                // Within range – constrained by the tighter side
                let liq_a =
                    est_liquidity_for_token_a(current_sqrt_price, upper_sqrt_price, token_max_a)?;
                let liq_b =
                    est_liquidity_for_token_b(current_sqrt_price, lower_sqrt_price, token_max_b)?;
                Ok(liq_a.min(liq_b))
            }
        }

        fn est_liquidity_for_token_a(
            sqrt_price_0: u128,
            sqrt_price_1: u128,
            token_amount_a: u64,
        ) -> Result<u128, MathError> {
            let (sqrt_price_lower, sqrt_price_upper) =
                increasing_price_order(sqrt_price_0, sqrt_price_1);
            let sqrt_price_diff = sqrt_price_upper - sqrt_price_lower;
            // this operation does not trigger overflow (MAX_SQRT_PRICE * MAX_SQRT_PRICE * u64::MAX < u256::MAX)
            let numerator_x128 =
                mul_u256(sqrt_price_upper, sqrt_price_lower).mul((token_amount_a).into());
            // Shift right by 64 bits to convert Q64.128 -> Q64.64
            let numerator_x64 = numerator_x128.shift_word_right();
            let (liquidity_u256, _) = numerator_x64.div(sqrt_price_diff.into(), false);
            liquidity_u256.try_into_u128()
        }

        fn est_liquidity_for_token_b(
            sqrt_price_0: u128,
            sqrt_price_1: u128,
            token_amount_b: u64,
        ) -> Result<u128, MathError> {
            let (sqrt_price_lower, sqrt_price_upper) =
                increasing_price_order(sqrt_price_0, sqrt_price_1);
            let sqrt_price_diff = sqrt_price_upper - sqrt_price_lower;
            let numerator_x64: u128 = (token_amount_b as u128) << 64;
            let liquidity_u128 = numerator_x64 / sqrt_price_diff;
            Ok(liquidity_u128)
        }
    }

    pub mod liquidity {
        use super::MathError;

        // Adds a signed liquidity delta to a given integer liquidity amount.
        // Errors on overflow or underflow.
        pub fn add_liquidity_delta(liquidity: u128, delta: i128) -> Result<u128, MathError> {
            if delta == 0 {
                return Ok(liquidity);
            }
            if delta > 0 {
                liquidity
                    .checked_add(delta as u128)
                    .ok_or(MathError::LiquidityOverflow)
            } else {
                liquidity
                    .checked_sub(delta.unsigned_abs())
                    .ok_or(MathError::LiquidityUnderflow)
            }
        }
    }

    pub mod swap {
        use super::bit::{checked_mul_div, checked_mul_div_round_up};
        use super::token::{
            get_amount_delta_a, get_amount_delta_b, get_next_sqrt_price, try_get_amount_delta_a,
            try_get_amount_delta_b, AmountDeltaU64, FEE_RATE_MUL_VALUE,
        };
        use super::MathError;
        use std::convert::TryInto;

        pub const NO_EXPLICIT_SQRT_PRICE_LIMIT: u128 = 0u128;

        #[derive(PartialEq, Debug)]
        pub struct SwapStepComputation {
            pub amount_in: u64,
            pub amount_out: u64,
            pub next_price: u128,
            pub fee_amount: u64,
        }

        pub fn compute_swap(
            amount_remaining: u64,
            fee_rate: u32,
            liquidity: u128,
            sqrt_price_current: u128,
            sqrt_price_target: u128,
            amount_specified_is_input: bool,
            a_to_b: bool,
        ) -> Result<SwapStepComputation, MathError> {
            // Since SplashPool (aka FullRange only pool) has only 2 initialized ticks at both ends,
            // the possibility of exceeding u64 when calculating "delta amount" is higher than concentrated pools.
            // This problem occurs with ExactIn.
            // The reason is that in ExactOut, "fixed delta" never exceeds the amount of tokens present in the pool and is clearly within the u64 range.
            // On the other hand, for ExactIn, "fixed delta" may exceed u64 because it calculates the amount of tokens needed to move the price to the end.
            // However, the primary purpose of initial calculation of "fixed delta" is to determine whether or not the iteration is "max swap" or not.
            // So the info that “the amount of tokens required exceeds the u64 range” is sufficient to determine that the iteration is NOT "max swap".
            //
            // delta <= u64::MAX: AmountDeltaU64::Valid
            // delta >  u64::MAX: AmountDeltaU64::ExceedsMax
            let initial_amount_fixed_delta = try_get_amount_fixed_delta(
                sqrt_price_current,
                sqrt_price_target,
                liquidity,
                amount_specified_is_input,
                a_to_b,
            )?;

            let mut amount_calc = amount_remaining;
            if amount_specified_is_input {
                amount_calc = checked_mul_div(
                    amount_remaining as u128,
                    FEE_RATE_MUL_VALUE - fee_rate as u128,
                    FEE_RATE_MUL_VALUE,
                )?
                .try_into()
                .map_err(|_| MathError::TokenMaxExceeded)?;
            }

            let next_sqrt_price = if initial_amount_fixed_delta.lte(amount_calc) {
                sqrt_price_target
            } else {
                get_next_sqrt_price(
                    sqrt_price_current,
                    liquidity,
                    amount_calc,
                    amount_specified_is_input,
                    a_to_b,
                )?
            };

            let is_max_swap = next_sqrt_price == sqrt_price_target;

            let amount_unfixed_delta = get_amount_unfixed_delta(
                sqrt_price_current,
                next_sqrt_price,
                liquidity,
                amount_specified_is_input,
                a_to_b,
            )?;

            // If the swap is not at the max, we need to readjust the amount of the fixed token we are using
            let amount_fixed_delta = if !is_max_swap || initial_amount_fixed_delta.exceeds_max() {
                // next_sqrt_price is calculated by get_next_sqrt_price and the result will be in the u64 range.
                get_amount_fixed_delta(
                    sqrt_price_current,
                    next_sqrt_price,
                    liquidity,
                    amount_specified_is_input,
                    a_to_b,
                )?
            } else {
                // the result will be in the u64 range.
                initial_amount_fixed_delta.value()
            };

            let (amount_in, mut amount_out) = if amount_specified_is_input {
                (amount_fixed_delta, amount_unfixed_delta)
            } else {
                (amount_unfixed_delta, amount_fixed_delta)
            };

            // Cap output amount if using output
            if !amount_specified_is_input && amount_out > amount_remaining {
                amount_out = amount_remaining;
            }

            let fee_amount = if amount_specified_is_input && !is_max_swap {
                amount_remaining - amount_in
            } else {
                checked_mul_div_round_up(
                    amount_in as u128,
                    fee_rate as u128,
                    FEE_RATE_MUL_VALUE - fee_rate as u128,
                )?
                .try_into()
                .map_err(|_| MathError::TokenMaxExceeded)?
            };

            Ok(SwapStepComputation {
                amount_in,
                amount_out,
                next_price: next_sqrt_price,
                fee_amount,
            })
        }

        fn get_amount_fixed_delta(
            sqrt_price_current: u128,
            sqrt_price_target: u128,
            liquidity: u128,
            amount_specified_is_input: bool,
            a_to_b: bool,
        ) -> Result<u64, MathError> {
            if a_to_b == amount_specified_is_input {
                get_amount_delta_a(
                    sqrt_price_current,
                    sqrt_price_target,
                    liquidity,
                    amount_specified_is_input,
                )
            } else {
                get_amount_delta_b(
                    sqrt_price_current,
                    sqrt_price_target,
                    liquidity,
                    amount_specified_is_input,
                )
            }
        }

        fn try_get_amount_fixed_delta(
            sqrt_price_current: u128,
            sqrt_price_target: u128,
            liquidity: u128,
            amount_specified_is_input: bool,
            a_to_b: bool,
        ) -> Result<AmountDeltaU64, MathError> {
            if a_to_b == amount_specified_is_input {
                try_get_amount_delta_a(
                    sqrt_price_current,
                    sqrt_price_target,
                    liquidity,
                    amount_specified_is_input,
                )
            } else {
                try_get_amount_delta_b(
                    sqrt_price_current,
                    sqrt_price_target,
                    liquidity,
                    amount_specified_is_input,
                )
            }
        }

        fn get_amount_unfixed_delta(
            sqrt_price_current: u128,
            sqrt_price_target: u128,
            liquidity: u128,
            amount_specified_is_input: bool,
            a_to_b: bool,
        ) -> Result<u64, MathError> {
            if a_to_b == amount_specified_is_input {
                get_amount_delta_b(
                    sqrt_price_current,
                    sqrt_price_target,
                    liquidity,
                    !amount_specified_is_input,
                )
            } else {
                get_amount_delta_a(
                    sqrt_price_current,
                    sqrt_price_target,
                    liquidity,
                    !amount_specified_is_input,
                )
            }
        }
    }

    pub use liquidity::add_liquidity_delta;
    pub use swap::compute_swap;
    pub use tick::{sqrt_price_from_tick_index, MAX_SQRT_PRICE_X64, MIN_SQRT_PRICE_X64};
    pub use token::FEE_RATE_MUL_VALUE;
}

fn mint_str_matches(mint: &str, pk: &Pubkey) -> bool {
    mint == pk.to_string().as_str()
}

fn resolve_a_to_b(input: &OrcaWhirlpoolQuoteInput, mint_in: &str, mint_out: &str) -> Option<bool> {
    if mint_str_matches(mint_in, &input.token_mint_a)
        && mint_str_matches(mint_out, &input.token_mint_b)
    {
        return Some(true);
    }
    if mint_str_matches(mint_in, &input.token_mint_b)
        && mint_str_matches(mint_out, &input.token_mint_a)
    {
        return Some(false);
    }
    None
}

struct TickArrayView<'a> {
    array: &'a ParsedTickArray,
}

impl<'a> TickArrayView<'a> {
    fn start_tick_index(&self) -> i32 {
        self.array.start_tick_index
    }

    fn in_search_range(&self, tick_index: i32, tick_spacing: u16, shifted: bool) -> bool {
        let mut lower = self.start_tick_index();
        let mut upper = self.start_tick_index() + TICK_ARRAY_SIZE * tick_spacing as i32;
        if shifted {
            lower -= tick_spacing as i32;
            upper -= tick_spacing as i32;
        }
        tick_index >= lower && tick_index < upper
    }

    fn tick_offset(&self, tick_index: i32, tick_spacing: u16) -> Option<isize> {
        if tick_index < self.start_tick_index() {
            return None;
        }
        let delta = tick_index - self.start_tick_index();
        if delta % tick_spacing as i32 != 0 {
            return None;
        }
        let offset = delta / tick_spacing as i32;
        if !(0..TICK_ARRAY_SIZE).contains(&offset) {
            return None;
        }
        Some(offset as isize)
    }

    fn get_next_init_tick_index(
        &self,
        tick_index: i32,
        tick_spacing: u16,
        a_to_b: bool,
    ) -> Result<Option<i32>, ()> {
        if !self.in_search_range(tick_index, tick_spacing, !a_to_b) {
            return Err(());
        }
        let mut curr_offset = self.tick_offset(tick_index, tick_spacing).ok_or(())? as i32;
        if !a_to_b {
            curr_offset += 1;
        }
        while (0..TICK_ARRAY_SIZE).contains(&curr_offset) {
            let slot = curr_offset as usize;
            if self.array.ticks[slot].initialized {
                return Ok(Some(
                    curr_offset * tick_spacing as i32 + self.start_tick_index(),
                ));
            }
            curr_offset = if a_to_b {
                curr_offset - 1
            } else {
                curr_offset + 1
            };
        }
        Ok(None)
    }

    fn get_tick(&self, tick_index: i32, tick_spacing: u16) -> Option<&ParsedTick> {
        let offset = self.tick_offset(tick_index, tick_spacing)?;
        Some(&self.array.ticks[offset as usize])
    }
}

struct SwapTickSequence<'a> {
    arrays: Vec<TickArrayView<'a>>,
}

impl<'a> SwapTickSequence<'a> {
    fn get_next_initialized_tick_index(
        &self,
        tick_index: i32,
        tick_spacing: u16,
        a_to_b: bool,
        start_array_index: usize,
    ) -> Result<(usize, i32), ()> {
        let ticks_in_array = TICK_ARRAY_SIZE * tick_spacing as i32;
        let mut search_index = tick_index;
        let mut array_index = start_array_index;
        loop {
            let next_array = self.arrays.get(array_index).ok_or(())?;
            match next_array.get_next_init_tick_index(search_index, tick_spacing, a_to_b) {
                Ok(Some(next_index)) => return Ok((array_index, next_index)),
                Ok(None) => {
                    if array_index + 1 == self.arrays.len() {
                        if a_to_b {
                            return Ok((array_index, next_array.start_tick_index()));
                        } else {
                            let last_tick = next_array.start_tick_index() + ticks_in_array
                                - tick_spacing as i32;
                            return Ok((array_index, last_tick));
                        }
                    }
                    let next_array_start = self
                        .arrays
                        .get(array_index + 1)
                        .ok_or(())?
                        .start_tick_index();
                    search_index = if a_to_b {
                        next_array_start + ticks_in_array - tick_spacing as i32
                    } else {
                        next_array_start
                    };
                    array_index += 1;
                }
                Err(()) => return Err(()),
            }
        }
    }

    fn get_tick_offset(
        &self,
        array_index: usize,
        tick_index: i32,
        tick_spacing: u16,
    ) -> Option<isize> {
        self.arrays
            .get(array_index)?
            .tick_offset(tick_index, tick_spacing)
    }
}

fn get_next_sqrt_prices(
    next_tick_index: i32,
    sqrt_price_limit: u128,
    a_to_b: bool,
) -> (u128, u128) {
    use whirlpool_math::sqrt_price_from_tick_index;
    let next_tick_price = sqrt_price_from_tick_index(next_tick_index);
    let next_sqrt_price_limit = if a_to_b {
        sqrt_price_limit.max(next_tick_price)
    } else {
        sqrt_price_limit.min(next_tick_price)
    };
    (next_tick_price, next_sqrt_price_limit)
}

fn try_quote_walk(
    pool: &OrcaWhirlpoolQuoteInput,
    sequence: &SwapTickSequence<'_>,
    a_to_b: bool,
    amount_in: u64,
) -> Result<u64, whirlpool_math::MathError> {
    use whirlpool_math::{
        add_liquidity_delta, compute_swap, MAX_SQRT_PRICE_X64, MIN_SQRT_PRICE_X64,
    };

    let sqrt_price_limit = if a_to_b {
        MIN_SQRT_PRICE_X64
    } else {
        MAX_SQRT_PRICE_X64
    };
    let fee_rate = pool.fee_rate as u32;
    let tick_spacing = pool.tick_spacing;

    let mut amount_remaining = amount_in;
    let mut amount_calculated: u64 = 0;
    let mut curr_sqrt_price = pool.sqrt_price;
    let mut curr_tick_index = pool.tick_current_index;
    let mut curr_liquidity = pool.liquidity;
    let mut curr_array_index: usize = 0;

    while amount_remaining > 0 && sqrt_price_limit != curr_sqrt_price {
        let (next_array_index, next_tick_index) = sequence
            .get_next_initialized_tick_index(
                curr_tick_index,
                tick_spacing,
                a_to_b,
                curr_array_index,
            )
            .map_err(|_| whirlpool_math::MathError::DivideByZero)?;

        let (next_tick_sqrt_price, sqrt_price_target) =
            get_next_sqrt_prices(next_tick_index, sqrt_price_limit, a_to_b);

        let swap_computation = compute_swap(
            amount_remaining,
            fee_rate,
            curr_liquidity,
            curr_sqrt_price,
            sqrt_price_target,
            true,
            a_to_b,
        )?;

        amount_remaining = amount_remaining
            .checked_sub(swap_computation.amount_in)
            .ok_or(whirlpool_math::MathError::MulDivOverflow)?;
        amount_remaining = amount_remaining
            .checked_sub(swap_computation.fee_amount)
            .ok_or(whirlpool_math::MathError::MulDivOverflow)?;
        amount_calculated = amount_calculated
            .checked_add(swap_computation.amount_out)
            .ok_or(whirlpool_math::MathError::MulDivOverflow)?;

        if swap_computation.next_price == next_tick_sqrt_price {
            if let Some(array) = sequence.arrays.get(next_array_index) {
                if let Some(tick) = array.get_tick(next_tick_index, tick_spacing) {
                    if tick.initialized {
                        let delta = if a_to_b {
                            -tick.liquidity_net
                        } else {
                            tick.liquidity_net
                        };
                        curr_liquidity = add_liquidity_delta(curr_liquidity, delta)?;
                    }
                }
            }

            let tick_offset = sequence
                .get_tick_offset(next_array_index, next_tick_index, tick_spacing)
                .unwrap_or(0);

            curr_array_index = if (a_to_b && tick_offset == 0)
                || (!a_to_b && tick_offset == TICK_ARRAY_SIZE as isize - 1)
            {
                next_array_index + 1
            } else {
                next_array_index
            };

            curr_tick_index = if a_to_b {
                next_tick_index - 1
            } else {
                next_tick_index
            };
        }

        curr_sqrt_price = swap_computation.next_price;

        if amount_remaining == 0 || curr_sqrt_price == sqrt_price_target {
            break;
        }
    }

    if amount_remaining > 0 {
        return Err(whirlpool_math::MathError::TokenMaxExceeded);
    }
    Ok(amount_calculated)
}

pub fn orca_quote_exact_in(
    pool: &OrcaWhirlpoolQuoteInput,
    ticks: &OrcaTickArrays,
    mint_in: &str,
    mint_out: &str,
    amount_in: u64,
) -> Option<u64> {
    if pool.sqrt_price == 0 || pool.liquidity == 0 || amount_in == 0 {
        return None;
    }
    let a_to_b = resolve_a_to_b(pool, mint_in, mint_out)?;
    let (s0, s1, s2) =
        swap_direction_tick_array_starts(pool.tick_current_index, pool.tick_spacing as i32, a_to_b);
    let arr0 = ticks.get(&s0)?;
    let arr1 = ticks.get(&s1)?;
    let arr2 = ticks.get(&s2)?;
    if arr0.whirlpool != pool.pool || arr1.whirlpool != pool.pool || arr2.whirlpool != pool.pool {
        return None;
    }

    let sequence = SwapTickSequence {
        arrays: vec![
            TickArrayView { array: arr0 },
            TickArrayView { array: arr1 },
            TickArrayView { array: arr2 },
        ],
    };

    let out = try_quote_walk(pool, &sequence, a_to_b, amount_in).ok()?;
    if out == 0 {
        None
    } else {
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::solana::dex::orca_tick_array::build_tick_array_account_bytes;

    fn sample_pool(
        sqrt_price: u128,
        liquidity: u128,
        tick: i32,
    ) -> (OrcaWhirlpoolQuoteInput, Pubkey, Pubkey) {
        let pool_pk = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let input = OrcaWhirlpoolQuoteInput {
            pool: pool_pk,
            token_mint_a: mint_a,
            token_mint_b: mint_b,
            sqrt_price,
            liquidity,
            tick_current_index: tick,
            tick_spacing: 64,
            fee_rate: 300,
        };
        (input, mint_a, mint_b)
    }

    fn insert_arrays(
        pool: &OrcaWhirlpoolQuoteInput,
        tick_now: i32,
        a_to_b: bool,
        updates: &[(i32, bool, i128)],
    ) -> OrcaTickArrays {
        let (s0, s1, s2) =
            swap_direction_tick_array_starts(tick_now, pool.tick_spacing as i32, a_to_b);
        let mut map = OrcaTickArrays::new();
        for start in [s0, s1, s2] {
            let bytes =
                build_tick_array_account_bytes(start, pool.pool, pool.tick_spacing, updates);
            let parsed =
                crate::solana::dex::orca_tick_array::parse_tick_array(&bytes).expect("parse");
            map.insert(start, parsed);
        }
        map
    }

    #[test]
    fn small_amount_in_no_cross_monotonic() {
        let tick = 0;
        let sqrt = whirlpool_math::sqrt_price_from_tick_index(tick);
        let liq = 1_000_000_000_000u128;
        let (pool, mint_a, mint_b) = sample_pool(sqrt, liq, tick);
        let ticks = insert_arrays(&pool, tick, true, &[]);
        let q1 = orca_quote_exact_in(
            &pool,
            &ticks,
            &mint_a.to_string(),
            &mint_b.to_string(),
            1_000,
        )
        .expect("q1");
        let q2 = orca_quote_exact_in(
            &pool,
            &ticks,
            &mint_a.to_string(),
            &mint_b.to_string(),
            10_000,
        )
        .expect("q2");
        assert!(q1 > 0);
        assert!(q2 > q1);
    }

    #[test]
    fn initialized_cross_changes_marginal_output() {
        let tick = 64 * 10;
        let sqrt = whirlpool_math::sqrt_price_from_tick_index(tick);
        let liq = 1_000_000_000_000u128;
        let (pool, _mint_a, _mint_b) = sample_pool(sqrt, liq, tick);
        let cross_tick = tick - pool.tick_spacing as i32;
        let ticks_no = insert_arrays(&pool, tick, true, &[]);
        let ticks_cross = insert_arrays(&pool, tick, true, &[(cross_tick, true, liq as i128)]);
        let (s0, s1, s2) = swap_direction_tick_array_starts(tick, pool.tick_spacing as i32, true);
        let slot = ((cross_tick - s0) / pool.tick_spacing as i32) as usize;
        assert!(ticks_cross.get(&s0).unwrap().ticks[slot].initialized);
        let seq_no = SwapTickSequence {
            arrays: vec![
                TickArrayView {
                    array: ticks_no.get(&s0).unwrap(),
                },
                TickArrayView {
                    array: ticks_no.get(&s1).unwrap(),
                },
                TickArrayView {
                    array: ticks_no.get(&s2).unwrap(),
                },
            ],
        };
        let (_, next_no) = seq_no
            .get_next_initialized_tick_index(tick, pool.tick_spacing, true, 0)
            .expect("next no");
        let seq_cross = SwapTickSequence {
            arrays: vec![
                TickArrayView {
                    array: ticks_cross.get(&s0).unwrap(),
                },
                TickArrayView {
                    array: ticks_cross.get(&s1).unwrap(),
                },
                TickArrayView {
                    array: ticks_cross.get(&s2).unwrap(),
                },
            ],
        };
        let (_, next_cross) = seq_cross
            .get_next_initialized_tick_index(tick, pool.tick_spacing, true, 0)
            .expect("next cross");
        assert_eq!(next_cross, cross_tick);
        assert_ne!(next_no, next_cross);
        let sqrt_no = whirlpool_math::sqrt_price_from_tick_index(next_no);
        let sqrt_cross = whirlpool_math::sqrt_price_from_tick_index(next_cross);
        assert!(
            sqrt_cross > sqrt_no,
            "init tick must bind swap earlier (higher sqrt) on a_to_b"
        );
        // Same small input stays in the same price segment (target is not binding yet).
        // Routing to the init tick still changes the marginal step vs an empty tick window.
        let step_cross = whirlpool_math::compute_swap(
            10_000_000_000,
            pool.fee_rate as u32,
            liq,
            sqrt,
            sqrt_cross,
            true,
            true,
        )
        .expect("step cross");
        let step_no = whirlpool_math::compute_swap(
            10_000_000_000,
            pool.fee_rate as u32,
            liq,
            sqrt,
            sqrt_no,
            true,
            true,
        )
        .expect("step no");
        assert_ne!(
            step_cross.amount_out, step_no.amount_out,
            "initialized cross must change the first-step marginal output"
        );
    }

    #[test]
    fn amount_in_exceeds_three_array_liquidity_returns_none() {
        let tick = 0;
        let sqrt = whirlpool_math::sqrt_price_from_tick_index(tick);
        let liq = 1_000u128;
        let (pool, mint_a, mint_b) = sample_pool(sqrt, liq, tick);
        let ticks = insert_arrays(&pool, tick, true, &[]);
        assert!(orca_quote_exact_in(
            &pool,
            &ticks,
            &mint_a.to_string(),
            &mint_b.to_string(),
            u64::MAX
        )
        .is_none());
    }

    fn cpmm_amount_out(amount_in: u64, sqrt_price: u128, fee_rate: u16, a_to_b: bool) -> u64 {
        let fee_adj = (amount_in as u128)
            .saturating_mul(whirlpool_math::FEE_RATE_MUL_VALUE - fee_rate as u128)
            / whirlpool_math::FEE_RATE_MUL_VALUE;
        let amount_in_after_fee = fee_adj as u64;
        if amount_in_after_fee == 0 {
            return 0;
        }
        let sqrt_f = sqrt_price as f64 / (1u128 << 64) as f64;
        let price = sqrt_f * sqrt_f;
        if a_to_b {
            (amount_in_after_fee as f64 * price).floor() as u64
        } else if price <= 0.0 {
            0
        } else {
            (amount_in_after_fee as f64 / price).floor() as u64
        }
    }

    #[test]
    fn walk_differs_from_local_cpmm_helper() {
        let tick = 128;
        let sqrt = whirlpool_math::sqrt_price_from_tick_index(tick);
        let liq = 800_000_000_000u128;
        let (pool, mint_a, mint_b) = sample_pool(sqrt, liq, tick);
        let ticks = insert_arrays(&pool, tick, true, &[(64, true, 500_000_000_000i128)]);
        let amount = 25_000_000u64;
        let walk = orca_quote_exact_in(
            &pool,
            &ticks,
            &mint_a.to_string(),
            &mint_b.to_string(),
            amount,
        )
        .expect("walk");
        let cpmm = cpmm_amount_out(amount, sqrt, pool.fee_rate, true);
        assert_ne!(walk, cpmm);
    }
}
