// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::borrow::Cow;

use crate::ScalarFunc;
use milevadb_query_datatype::codec::error::Error;
use milevadb_query_datatype::codec::mysql::time::extension::DateTimeExtension;
use milevadb_query_datatype::codec::mysql::time::weekmode::WeekMode;
use milevadb_query_datatype::codec::mysql::{Duration as MyDuration, Time, TimeType};
use milevadb_query_datatype::codec::Datum;
use milevadb_query_datatype::expr::SqlMode;
use milevadb_query_datatype::expr::{EvalContext, Result};

impl ScalarFunc {
    #[inline]
    pub fn date_format<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let t: Cow<'a, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.invalid_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        }
        let format_mask: Cow<'a, str> = try_opt!(self.children[1].eval_string_and_decode(ctx, Evcausetidx));
        let t = t.date_format(&format_mask);
        if let Err(err) = t {
            return ctx.handle_invalid_time_error(err).map(|_| None);
        }
        Ok(Some(Cow::Owned(t.unwrap().into_bytes())))
    }

    #[inline]
    pub fn date<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let mut t: Cow<'a, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.is_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        }
        let mut res = *t.to_mut();
        res.set_time_type(TimeType::Date)?;
        Ok(Some(Cow::Owned(res)))
    }

    #[inline]
    pub fn hour(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let dur = try_opt!(self.children[0].eval_duration(ctx, Evcausetidx));
        Ok(Some(i64::from(dur.hours())))
    }

    #[inline]
    pub fn minute(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let dur = try_opt!(self.children[0].eval_duration(ctx, Evcausetidx));
        Ok(Some(i64::from(dur.minutes())))
    }

    #[inline]
    pub fn second(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let dur = try_opt!(self.children[0].eval_duration(ctx, Evcausetidx));
        Ok(Some(i64::from(dur.secs())))
    }

    #[inline]
    pub fn time_to_sec(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let dur = try_opt!(self.children[0].eval_duration(ctx, Evcausetidx));
        Ok(Some(dur.to_secs()))
    }

    #[inline]
    pub fn micro_second(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let dur = try_opt!(self.children[0].eval_duration(ctx, Evcausetidx));
        Ok(Some(i64::from(dur.subsec_micros())))
    }

    #[inline]
    pub fn month<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<i64>> {
        let t: Cow<'a, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.is_zero() {
            if ctx.causet.sql_mode.contains(SqlMode::NO_ZERO_DATE) {
                return ctx
                    .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                    .map(|_| None);
            }
            return Ok(Some(0));
        }
        Ok(Some(i64::from(t.month())))
    }

    #[inline]
    pub fn month_name<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let t: Cow<'a, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        let month = t.month() as usize;
        if t.is_zero() && ctx.causet.sql_mode.contains(SqlMode::NO_ZERO_DATE) {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        } else if month == 0 || t.is_zero() {
            return Ok(None);
        }
        use milevadb_query_datatype::codec::mysql::time::MONTH_NAMES;
        Ok(Some(Cow::Owned(
            MONTH_NAMES[month - 1].to_string().into_bytes(),
        )))
    }

    #[inline]
    pub fn day_name<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let t: Cow<'a, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.is_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        }
        use milevadb_query_datatype::codec::mysql::time::WeekdayExtension;
        let weekday = t.weekday();
        Ok(Some(Cow::Owned(weekday.name().to_string().into_bytes())))
    }

    #[inline]
    pub fn day_of_month(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let t: Cow<'_, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.is_zero() {
            if ctx.causet.sql_mode.contains(SqlMode::NO_ZERO_DATE) {
                return ctx
                    .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                    .map(|_| None);
            }
            return Ok(Some(0));
        }
        let day = t.day();
        Ok(Some(i64::from(day)))
    }

    #[inline]
    pub fn day_of_week(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let t: Cow<'_, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.is_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        }
        let day = t.weekday().number_from_sunday();
        Ok(Some(i64::from(day)))
    }

    #[inline]
    pub fn day_of_year(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let t: Cow<'_, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.is_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        }
        let day = t.days();
        Ok(Some(i64::from(day)))
    }

    #[inline]
    pub fn year(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let t: Cow<'_, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.is_zero() {
            if ctx.causet.sql_mode.contains(SqlMode::NO_ZERO_DATE) {
                return ctx
                    .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                    .map(|_| None);
            }
            return Ok(Some(0));
        }
        Ok(Some(i64::from(t.year())))
    }

    #[inline]
    pub fn last_day<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let mut t: Cow<'a, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.month() == 0 {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        }
        let res = *t.to_mut();
        Ok(res.last_date_of_month().map(Cow::Owned))
    }

    #[inline]
    pub fn week_with_mode(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let t: Cow<'_, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.is_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        }
        let mode: i64 = try_opt!(self.children[1].eval_int(ctx, Evcausetidx));
        let week = t.week(WeekMode::from_bits_truncate(mode as u32));
        Ok(Some(i64::from(week)))
    }

    #[inline]
    pub fn week_without_mode(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let t: Cow<'_, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.is_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        }
        let week = t.week(WeekMode::from_bits_truncate(0u32));
        Ok(Some(i64::from(week)))
    }

    #[inline]
    pub fn week_day(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let t: Cow<'_, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.is_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        }
        let day = t.weekday().num_days_from_monday();
        Ok(Some(i64::from(day)))
    }

    #[inline]
    pub fn week_of_year(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let t: Cow<'_, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.is_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        }
        Ok(Some(i64::from(t.week(WeekMode::from_bits_truncate(3)))))
    }

    #[inline]
    pub fn year_week_with_mode(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let t: Cow<'_, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.is_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        }
        let mode = match self.children[1].eval_int(ctx, Evcausetidx) {
            Err(e) => return Err(e),
            Ok(None) => 0,
            Ok(Some(num)) => num,
        };
        let (year, week) = t.year_week(WeekMode::from_bits_truncate(mode as u32));
        let mut result = i64::from(week + year * 100);
        if result < 0 {
            result = i64::from(u32::max_value());
        }
        Ok(Some(result))
    }

    #[inline]
    pub fn year_week_without_mode(
        &self,
        ctx: &mut EvalContext,
        Evcausetidx: &[Datum],
    ) -> Result<Option<i64>> {
        let t: Cow<'_, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.is_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        }
        let (year, week) = t.year_week(WeekMode::from_bits_truncate(0u32));
        let mut result = i64::from(week + year * 100);
        if result < 0 {
            result = i64::from(u32::max_value());
        }
        Ok(Some(result))
    }

    pub fn period_add(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let p = try_opt!(self.children[0].eval_int(ctx, Evcausetidx));
        if p == 0 {
            return Ok(Some(0));
        }
        let n = try_opt!(self.children[1].eval_int(ctx, Evcausetidx));
        let (month, _) = (i64::from(Time::period_to_month(p as u64) as i32)).overflowing_add(n);
        Ok(Some(Time::month_to_period(u64::from(month as u32)) as i64))
    }

    pub fn period_diff(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let p1 = try_opt!(self.children[0].eval_int(ctx, Evcausetidx));
        let p2 = try_opt!(self.children[1].eval_int(ctx, Evcausetidx));
        Ok(Some(
            Time::period_to_month(p1 as u64) as i64 - Time::period_to_month(p2 as u64) as i64,
        ))
    }

    #[inline]
    pub fn to_days(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let t: Cow<'_, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.invalid_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        }
        Ok(Some(i64::from(t.day_number())))
    }

    #[inline]
    pub fn to_seconds(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let t: Cow<'_, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if t.invalid_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
                .map(|_| None);
        }
        Ok(Some(t.second_number()))
    }

    #[inline]
    pub fn date_diff(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let lhs: Cow<'_, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        if lhs.invalid_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", lhs)))
                .map(|_| None);
        }
        let rhs: Cow<'_, Time> = try_opt!(self.children[1].eval_time(ctx, Evcausetidx));
        if rhs.invalid_zero() {
            return ctx
                .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", rhs)))
                .map(|_| None);
        }
        Ok(lhs.date_diff(rhs.into_owned()))
    }

    #[inline]
    pub fn add_datetime_and_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let arg0: Cow<'a, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        let arg1 = try_opt!(self.children[1].eval_duration(ctx, Evcausetidx));
        let overflow = Error::overflow("TIME", &format!("({} + {})", &arg0, &arg1));
        let mut res = match arg0.into_owned().checked_add(ctx, arg1) {
            Some(res) => res,
            None => return Err(overflow),
        };
        res.set_time_type(TimeType::DateTime)?;
        Ok(Some(Cow::Owned(res)))
    }

    #[inline]
    pub fn add_datetime_and_string<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let arg0: Cow<'a, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        let arg1: Cow<'a, [u8]> = try_opt!(self.children[1].eval_string(ctx, Evcausetidx));
        let s = std::str::from_utf8(&arg1)?;
        let arg1 = match MyDuration::parse(ctx, &arg1, Time::parse_fsp(s)) {
            Ok(arg1) => arg1,
            Err(_) => return Ok(None),
        };
        let overflow = Error::overflow("TIME", &format!("({} + {})", &arg0, &arg1));
        let mut res = match arg0.into_owned().checked_add(ctx, arg1) {
            Some(res) => res,
            None => return Err(overflow),
        };
        res.set_time_type(TimeType::DateTime)?;
        Ok(Some(Cow::Owned(res)))
    }

    #[inline]
    pub fn add_time_datetime_null<'a>(
        &self,
        _ctx: &mut EvalContext,
        _row: &[Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        Ok(None)
    }

    #[inline]
    pub fn add_duration_and_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<MyDuration>> {
        let arg0 = try_opt!(self.children[0].eval_duration(ctx, Evcausetidx));
        let arg1 = try_opt!(self.children[1].eval_duration(ctx, Evcausetidx));
        let overflow = Error::overflow("DURATION", &format!("({} + {})", &arg0, &arg1));
        let res = match arg0.checked_add(arg1) {
            Some(res) => res,
            None => return Err(overflow),
        };
        Ok(Some(res))
    }

    #[inline]
    pub fn add_duration_and_string<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<MyDuration>> {
        let arg0 = try_opt!(self.children[0].eval_duration(ctx, Evcausetidx));
        let arg1: Cow<'a, [u8]> = try_opt!(self.children[1].eval_string(ctx, Evcausetidx));
        let s = std::str::from_utf8(&arg1)?;
        let arg1 = match MyDuration::parse(ctx, &arg1, Time::parse_fsp(s)) {
            Ok(arg1) => arg1,
            Err(_) => return Ok(None),
        };
        let overflow = Error::overflow("DURATION", &format!("({} + {})", &arg0, &arg1));
        let res = match arg0.checked_add(arg1) {
            Some(res) => res,
            None => return Err(overflow),
        };
        Ok(Some(res))
    }

    #[inline]
    pub fn add_time_duration_null(
        &self,
        _ctx: &mut EvalContext,
        _row: &[Datum],
    ) -> Result<Option<MyDuration>> {
        Ok(None)
    }

    #[inline]
    pub fn add_date_and_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let date = try_opt!(self.children[0].eval_duration(ctx, Evcausetidx));
        let duration = try_opt!(self.children[1].eval_duration(ctx, Evcausetidx));
        let overflow = Error::overflow("DURATION", &format!("({} - {})", &date, &duration));
        let res = date.checked_add(duration).ok_or(overflow)?;
        Ok(Some(Cow::Owned(res.to_string().into_bytes())))
    }

    #[inline]
    pub fn add_date_and_string<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let date = try_opt!(self.children[0].eval_duration(ctx, Evcausetidx));
        let string = try_opt!(self.children[1].eval_string(ctx, Evcausetidx));
        let s = std::str::from_utf8(&string)?;
        let string = match MyDuration::parse(ctx, &string, Time::parse_fsp(s)) {
            Ok(string) => string,
            Err(_) => return Ok(None),
        };
        let overflow = Error::overflow("DURATION", &format!("({} - {})", &date, &string));
        let res = date.checked_add(string).ok_or(overflow)?;
        Ok(Some(Cow::Owned(res.to_string().into_bytes())))
    }

    #[inline]
    pub fn sub_datetime_and_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let arg0: Cow<'a, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        let arg1 = try_opt!(self.children[1].eval_duration(ctx, Evcausetidx));
        let overflow = Error::overflow("TIME", &format!("({} - {})", &arg0, &arg1));
        let mut res = match arg0.into_owned().checked_sub(ctx, arg1) {
            Some(res) => res,
            None => return Err(overflow),
        };
        res.set_time_type(TimeType::DateTime)?;
        Ok(Some(Cow::Owned(res)))
    }

    #[inline]
    pub fn sub_datetime_and_string<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let arg0: Cow<'a, Time> = try_opt!(self.children[0].eval_time(ctx, Evcausetidx));
        let arg1: Cow<'a, [u8]> = try_opt!(self.children[1].eval_string(ctx, Evcausetidx));
        let s = std::str::from_utf8(&arg1)?;
        let arg1 = match MyDuration::parse(ctx, &arg1, Time::parse_fsp(s)) {
            Ok(arg1) => arg1,
            Err(_) => return Ok(None),
        };
        let overflow = Error::overflow("TIME", &format!("({} - {})", &arg0, &arg1));
        let mut res = match arg0.into_owned().checked_sub(ctx, arg1) {
            Some(res) => res,
            None => return Err(overflow),
        };
        res.set_time_type(TimeType::DateTime)?;
        Ok(Some(Cow::Owned(res)))
    }

    #[inline]
    pub fn sub_time_datetime_null<'a>(
        &self,
        _ctx: &mut EvalContext,
        _row: &[Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        Ok(None)
    }

    #[inline]
    pub fn sub_duration_and_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<MyDuration>> {
        let d0 = try_opt!(self.children[0].eval_duration(ctx, Evcausetidx));
        let d1 = try_opt!(self.children[1].eval_duration(ctx, Evcausetidx));
        let diff = match d0.to_nanos().checked_sub(d1.to_nanos()) {
            Some(result) => result,
            None => return Err(Error::overflow("DURATION", &format!("({} - {})", &d0, &d1))),
        };
        let res = MyDuration::from_nanos(diff, d0.fsp().max(d1.fsp()) as i8)?;
        Ok(Some(res))
    }

    #[inline]
    pub fn sub_duration_and_string<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<MyDuration>> {
        let arg0 = try_opt!(self.children[0].eval_duration(ctx, Evcausetidx));
        let arg1: Cow<'a, [u8]> = try_opt!(self.children[1].eval_string(ctx, Evcausetidx));
        let s = std::str::from_utf8(&arg1)?;
        let arg1 = match MyDuration::parse(ctx, &arg1, Time::parse_fsp(s)) {
            Ok(arg1) => arg1,
            Err(_) => return Ok(None),
        };
        let overflow = Error::overflow("DURATION", &format!("({} - {})", &arg0, &arg1));
        let res = match arg0.checked_sub(arg1) {
            Some(res) => res,
            None => return Err(overflow),
        };
        Ok(Some(res))
    }

    #[inline]
    pub fn sub_time_duration_null(
        &self,
        _ctx: &mut EvalContext,
        _row: &[Datum],
    ) -> Result<Option<MyDuration>> {
        Ok(None)
    }

    #[inline]
    pub fn sub_date_and_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let date = try_opt!(self.children[0].eval_duration(ctx, Evcausetidx));
        let duration = try_opt!(self.children[1].eval_duration(ctx, Evcausetidx));
        let overflow = Error::overflow("DURATION", &format!("({} - {})", &date, &duration));
        let res = date.checked_sub(duration).ok_or(overflow)?;
        Ok(Some(Cow::Owned(res.to_string().into_bytes())))
    }

    #[inline]
    pub fn sub_date_and_string<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let date = try_opt!(self.children[0].eval_duration(ctx, Evcausetidx));
        let string = try_opt!(self.children[1].eval_string(ctx, Evcausetidx));
        let s = std::str::from_utf8(&string)?;
        let string = match MyDuration::parse(ctx, &string, Time::parse_fsp(s)) {
            Ok(string) => string,
            Err(_) => return Ok(None),
        };
        let overflow = Error::overflow("DURATION", &format!("({} - {})", &date, &string));
        let res = date.checked_sub(string).ok_or(overflow)?;
        Ok(Some(Cow::Owned(res.to_string().into_bytes())))
    }

    #[inline]
    pub fn add_time_string_null<'a>(
        &self,
        _ctx: &mut EvalContext,
        _row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        Ok(None)
    }

    #[inline]
    pub fn from_days<'a>(
        &self,
        ctx: &mut EvalContext,
        Evcausetidx: &[Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let days = try_opt!(self.children[0].eval_int(ctx, Evcausetidx)) as u32;
        let time = Time::from_days(ctx, days)?;
        Ok(Some(Cow::Owned(time)))
    }

    #[inline]
    pub fn make_date<'a>(
        &self,
        ctx: &mut EvalContext,
        Evcausetidx: &[Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let mut year = try_opt!(self.children[0].eval_int(ctx, Evcausetidx));
        let mut day = try_opt!(self.children[1].eval_int(ctx, Evcausetidx));
        if day <= 0 || year < 0 || year > 9999 || day > 366 * 9999 {
            return Ok(None);
        }
        if year < 70 {
            year += 2000;
        } else if year < 100 {
            year += 1900;
        }
        year -= 1;
        let d4 = year / 4;
        let d100 = year / 100;
        let d400 = year / 400;
        let leap = d4 - d100 + d400;
        day = day + leap + year * 365 + 365;
        let days = day as u32;
        let ret = Time::from_days(ctx, days)?;
        if ret.year() > 9999 || ret.is_zero() {
            return Ok(None);
        }
        Ok(Some(Cow::Owned(ret)))
    }
}

#[causet(test)]
mod tests {
    use std::sync::Arc;

    use fidel_timeshare::{Expr, ScalarFuncSig};

    use crate::tests::{datum_expr, scalar_func_expr};
    use crate::*;
    use milevadb_query_datatype::codec::mysql::{Duration, Time};
    use milevadb_query_datatype::codec::Datum;
    use milevadb_query_datatype::expr::{EvalConfig, EvalContext, Flag, SqlMode};

    fn expr_build(ctx: &mut EvalContext, sig: ScalarFuncSig, children: &[Expr]) -> Result<Datum> {
        let f = scalar_func_expr(sig, children);
        let op = Expression::build(ctx, f).unwrap();
        op.eval(ctx, &[])
    }

    fn test_ok_case_zero_arg(ctx: &mut EvalContext, sig: ScalarFuncSig, exp: Datum) {
        match expr_build(ctx, sig, &[]) {
            Ok(got) => assert_eq!(got, exp),
            Err(_) => panic!("eval failed"),
        }
    }

    fn test_ok_case_one_arg(ctx: &mut EvalContext, sig: ScalarFuncSig, arg: Datum, exp: Datum) {
        let children = &[datum_expr(arg)];
        match expr_build(ctx, sig, children) {
            Ok(got) => assert_eq!(got, exp),
            Err(_) => panic!("eval failed"),
        }
    }

    fn test_err_case_one_arg(ctx: &mut EvalContext, sig: ScalarFuncSig, arg: Datum) {
        let children = &[datum_expr(arg)];
        if let Ok(got) = expr_build(ctx, sig, children) {
            assert_eq!(got, Datum::Null);
        }
    }

    fn test_ok_case_two_arg(
        ctx: &mut EvalContext,
        sig: ScalarFuncSig,
        arg1: Datum,
        arg2: Datum,
        exp: Datum,
    ) {
        let children = &[datum_expr(arg1), datum_expr(arg2)];
        match expr_build(ctx, sig, children) {
            Ok(got) => assert_eq!(got, exp),
            Err(_) => panic!("eval failed"),
        }
    }

    fn test_err_case_two_arg(ctx: &mut EvalContext, sig: ScalarFuncSig, arg1: Datum, arg2: Datum) {
        let children = &[datum_expr(arg1), datum_expr(arg2)];
        if let Ok(got) = expr_build(ctx, sig, children) {
            assert_eq!(got, Datum::Null);
        }
    }

    #[test]
    fn test_date_format() {
        let cases = vec![
            (
                "2010-01-07 23:12:34.12345",
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u
                %V %v %a %W %w %X %x %Y %y %%",
                "Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01
                01 01 Thu Thursday 4 2010 2010 2010 10 %",
            ),
            (
                "2012-12-21 23:12:34.123456",
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U
                %u %V %v %a %W %w %X %x %Y %y %%",
                "Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51
                51 51 51 Fri Friday 5 2012 2012 2012 12 %",
            ),
            (
                "0000-01-01 00:00:00.123456",
                // Functions week() and yearweek() don't support multi mode,
                // so the result of "%U %u %V %Y" is different from MySQL.
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v
                %x %Y %y %%",
                "Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 52
                4294967295 0000 00 %",
            ),
            (
                "2016-09-3 00:59:59.123456",
                "abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U
                %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z",
                "abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35
                35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z",
            ),
            (
                "2012-10-01 00:00:00",
                "%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v
                %x %Y %y %%",
                "Oct October 10 10 1st 01 1 275 0 00 00 AM 12:00:00 AM 00:00:00 00 000000 40
                2012 2012 12 %",
            ),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            let datetime = Time::parse_datetime(&mut ctx, arg1, 6, true).unwrap();
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::DateFormatSig,
                Datum::Time(datetime),
                Datum::Bytes(arg2.to_string().into_bytes()),
                Datum::Bytes(exp.to_string().into_bytes()),
            );
        }
        // test NULL case
        test_err_case_two_arg(
            &mut ctx,
            ScalarFuncSig::DateFormatSig,
            Datum::Null,
            Datum::Null,
        );
        // test zero case
        let mut causet = EvalConfig::new();
        causet.set_flag(Flag::IN_fidelio_OR_DELETE_STMT)
            .set_sql_mode(SqlMode::ERROR_FOR_DIVISION_BY_ZERO | SqlMode::STRICT_ALL_BlockS);
        ctx = EvalContext::new(Arc::new(causet));
        test_err_case_two_arg(
            &mut ctx,
            ScalarFuncSig::DateFormatSig,
            Datum::Null,
            Datum::Null,
        );
    }

    #[test]
    fn test_date() {
        let cases = vec![
            ("2011-11-11", "2011-11-11"),
            ("2011-11-11 10:10:10", "2011-11-11"),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datum_arg = Datum::Time(Time::parse_datetime(&mut ctx, arg, 6, true).unwrap());
            let datum_exp = Datum::Time(Time::parse_datetime(&mut ctx, exp, 6, true).unwrap());
            test_ok_case_one_arg(&mut ctx, ScalarFuncSig::Date, datum_arg, datum_exp);
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::Date, Datum::Null);
        // test zero case
        let mut causet = EvalConfig::new();
        causet.set_flag(Flag::IN_fidelio_OR_DELETE_STMT)
            .set_sql_mode(SqlMode::ERROR_FOR_DIVISION_BY_ZERO | SqlMode::STRICT_ALL_BlockS);
        ctx = EvalContext::new(Arc::new(causet));
        let datetime =
            Datum::Time(Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 6, true).unwrap());
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::Date, datetime);
    }

    #[test]
    fn test_hour_min_sec_micro_sec() {
        // test hour, minute, second, micro_second
        let cases: Vec<(&str, i8, i64, i64, i64, i64)> = vec![
            ("31 11:30:45", 0, 31 * 24 + 11, 30, 45, 0),
            ("11:30:45.123345", 3, 11, 30, 45, 123000),
            ("11:30:45.123345", 5, 11, 30, 45, 123350),
            ("11:30:45.123345", 6, 11, 30, 45, 123345),
            ("11:30:45.1233456", 6, 11, 30, 45, 123346),
            ("11:30:45.000010", 6, 11, 30, 45, 10),
            ("11:30:45.00010", 5, 11, 30, 45, 100),
            ("-11:30:45.9233456", 0, 11, 30, 46, 0),
            ("-11:30:45.9233456", 1, 11, 30, 45, 900000),
            ("272:59:59.94", 2, 272, 59, 59, 940000),
            ("272:59:59.99", 1, 273, 0, 0, 0),
            ("272:59:59.99", 0, 273, 0, 0, 0),
        ];
        let mut ctx = EvalContext::default();
        for (arg, fsp, h, m, s, ms) in cases {
            let d = Datum::Dur(Duration::parse(&mut ctx, arg.as_bytes(), fsp).unwrap());
            test_ok_case_one_arg(&mut ctx, ScalarFuncSig::Hour, d.clone(), Datum::I64(h));
            test_ok_case_one_arg(&mut ctx, ScalarFuncSig::Minute, d.clone(), Datum::I64(m));
            test_ok_case_one_arg(&mut ctx, ScalarFuncSig::Second, d.clone(), Datum::I64(s));
            test_ok_case_one_arg(&mut ctx, ScalarFuncSig::MicroSecond, d, Datum::I64(ms));
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::Hour, Datum::Null);
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::Minute, Datum::Null);
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::Second, Datum::Null);
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::MicroSecond, Datum::Null);
        // test zero case
        let d = Datum::Dur(Duration::parse(&mut ctx, b"0 00:00:00.0", 0).unwrap());
        test_ok_case_one_arg(&mut ctx, ScalarFuncSig::Hour, d.clone(), Datum::I64(0));
        test_ok_case_one_arg(&mut ctx, ScalarFuncSig::Minute, d.clone(), Datum::I64(0));
        test_ok_case_one_arg(&mut ctx, ScalarFuncSig::Second, d.clone(), Datum::I64(0));
        test_ok_case_one_arg(&mut ctx, ScalarFuncSig::MicroSecond, d, Datum::I64(0));
    }

    #[test]
    fn test_time_to_sec() {
        // test time_to_sec
        let cases: Vec<(&str, i8, i64)> = vec![
            ("31 11:30:45", 0, 2719845),
            ("11:30:45.123345", 3, 41445),
            ("-11:30:45.1233456", 0, -41445),
            ("272:59:59.14", 0, 982799),
        ];
        let mut ctx = EvalContext::default();
        for (arg, fsp, s) in cases {
            let d = Datum::Dur(Duration::parse(&mut ctx, arg.as_bytes(), fsp).unwrap());
            test_ok_case_one_arg(&mut ctx, ScalarFuncSig::TimeToSec, d, Datum::I64(s));
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::TimeToSec, Datum::Null);
        // test zero case
        let d = Datum::Dur(Duration::parse(&mut ctx, b"0 00:00:00.0", 0).unwrap());
        test_ok_case_one_arg(&mut ctx, ScalarFuncSig::MicroSecond, d, Datum::I64(0));
    }

    #[test]
    fn test_month() {
        let cases = vec![
            ("0000-00-00 00:00:00", 0i64),
            ("2018-01-01 01:01:01", 1i64),
            ("2018-02-01 01:01:01", 2i64),
            ("2018-03-01 01:01:01", 3i64),
            ("2018-04-01 01:01:01", 4i64),
            ("2018-05-01 01:01:01", 5i64),
            ("2018-06-01 01:01:01", 6i64),
            ("2018-07-01 01:01:01", 7i64),
            ("2018-08-01 01:01:01", 8i64),
            ("2018-09-01 01:01:01", 9i64),
            ("2018-10-01 01:01:01", 10i64),
            ("2018-11-01 01:01:01", 11i64),
            ("2018-12-01 01:01:01", 12i64),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Time::parse_datetime(&mut ctx, arg, 6, true).unwrap();
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::Month,
                Datum::Time(datetime),
                Datum::I64(exp),
            );
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::Month, Datum::Null);
    }

    #[test]
    fn test_month_name() {
        let mut ctx = EvalContext::default();
        let cases = vec![
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00.000000", 6, true).unwrap(),
                ),
                Datum::Null,
            ),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2018-01-01 00:00:00.000000", 6, true).unwrap(),
                ),
                Datum::Bytes(b"January".to_vec()),
            ),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2018-02-01 00:00:00.000000", 6, true).unwrap(),
                ),
                Datum::Bytes(b"February".to_vec()),
            ),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2018-03-01 00:00:00.000000", 6, true).unwrap(),
                ),
                Datum::Bytes(b"March".to_vec()),
            ),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2018-04-01 00:00:00.000000", 6, true).unwrap(),
                ),
                Datum::Bytes(b"April".to_vec()),
            ),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2018-05-01 00:00:00.000000", 6, true).unwrap(),
                ),
                Datum::Bytes(b"May".to_vec()),
            ),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2018-06-01 00:00:00.000000", 6, true).unwrap(),
                ),
                Datum::Bytes(b"June".to_vec()),
            ),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2018-07-01 00:00:00.000000", 6, true).unwrap(),
                ),
                Datum::Bytes(b"July".to_vec()),
            ),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2018-08-01 00:00:00.000000", 6, true).unwrap(),
                ),
                Datum::Bytes(b"August".to_vec()),
            ),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2018-09-01 00:00:00.000000", 6, true).unwrap(),
                ),
                Datum::Bytes(b"September".to_vec()),
            ),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2018-10-01 00:00:00.000000", 6, true).unwrap(),
                ),
                Datum::Bytes(b"October".to_vec()),
            ),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2018-11-01 00:00:00.000000", 6, true).unwrap(),
                ),
                Datum::Bytes(b"November".to_vec()),
            ),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2018-12-01 00:00:00.000000", 6, true).unwrap(),
                ),
                Datum::Bytes(b"December".to_vec()),
            ),
        ];
        for (arg, exp) in cases {
            test_ok_case_one_arg(&mut ctx, ScalarFuncSig::MonthName, arg, exp);
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::MonthName, Datum::Null);
    }

    #[test]
    fn test_day_name() {
        let cases = vec![
            ("2018-11-11 00:00:00.000000", "Sunday"),
            ("2018-11-12 00:00:00.000000", "Monday"),
            ("2018-11-13 00:00:00.000000", "Tuesday"),
            ("2018-11-14 00:00:00.000000", "Wednesday"),
            ("2018-11-15 00:00:00.000000", "Thursday"),
            ("2018-11-16 00:00:00.000000", "Friday"),
            ("2018-11-17 00:00:00.000000", "Saturday"),
            ("2018-11-18 00:00:00.000000", "Sunday"),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Time::parse_datetime(&mut ctx, arg, 6, true).unwrap();
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::DayName,
                Datum::Time(datetime),
                Datum::Bytes(exp.as_bytes().to_vec()),
            );
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::DayName, Datum::Null);
        //  test zero case
        let zero_datetime =
            Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 6, false).unwrap();
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::DayName, Datum::Time(zero_datetime));
    }

    #[test]
    fn test_day_of_month() {
        let cases = vec![
            ("0000-00-00 00:00:00.000000", 0),
            ("2018-02-01 00:00:00.000000", 1),
            ("2018-02-15 00:00:00.000000", 15),
            ("2018-02-28 00:00:00.000000", 28),
            ("2016-02-29 00:00:00.000000", 29),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Time::parse_datetime(&mut ctx, arg, 6, false).unwrap();
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::DayOfMonth,
                Datum::Time(datetime),
                Datum::I64(exp),
            );
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::DayOfMonth, Datum::Null);
    }

    #[test]
    fn test_day_of_week() {
        let cases = vec![
            ("2018-11-11 00:00:00.000000", 1),
            ("2018-11-12 00:00:00.000000", 2),
            ("2018-11-13 00:00:00.000000", 3),
            ("2018-11-14 00:00:00.000000", 4),
            ("2018-11-15 00:00:00.000000", 5),
            ("2018-11-16 00:00:00.000000", 6),
            ("2018-11-17 00:00:00.000000", 7),
            ("2018-11-18 00:00:00.000000", 1),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Time::parse_datetime(&mut ctx, arg, 6, false).unwrap();
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::DayOfWeek,
                Datum::Time(datetime),
                Datum::I64(exp),
            );
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::DayOfWeek, Datum::Null);
        //  test zero case
        let datetime = Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 6, false).unwrap();
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::DayOfWeek, Datum::Time(datetime));
    }

    #[test]
    fn test_day_of_year() {
        let cases = vec![
            ("2018-11-11 00:00:00.000000", 315),
            ("2018-11-12 00:00:00.000000", 316),
            ("2018-11-30 00:00:00.000000", 334),
            ("2018-12-31 00:00:00.000000", 365),
            ("2016-12-31 00:00:00.000000", 366),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Time::parse_datetime(&mut ctx, arg, 6, true).unwrap();
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::DayOfYear,
                Datum::Time(datetime),
                Datum::I64(exp),
            );
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::DayOfYear, Datum::Null);
        //  test zero case
        let datetime = Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 6, true).unwrap();
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::DayOfYear, Datum::Time(datetime));
    }

    #[test]
    fn test_last_day() {
        let cases = vec![
            ("2011-11-11", "2011-11-30"),
            ("2008-02-10", "2008-02-29"),
            ("2000-02-11", "2000-02-29"),
            ("2100-02-11", "2100-02-28"),
            ("2011-11-11", "2011-11-30"),
            ("2011-11-11 10:10:10", "2011-11-30 00:00:00"),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datum_arg = Datum::Time(Time::parse_datetime(&mut ctx, arg, 6, true).unwrap());
            let datum_exp = Datum::Time(Time::parse_datetime(&mut ctx, exp, 6, true).unwrap());
            test_ok_case_one_arg(&mut ctx, ScalarFuncSig::LastDay, datum_arg, datum_exp);
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::LastDay, Datum::Null);
        // test zero case
        let datetime = Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 6, true).unwrap();
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::LastDay, Datum::Time(datetime));
    }

    #[test]
    fn test_year() {
        let cases = vec![
            ("0000-00-00 00:00:00", 0i64),
            ("1-01-01 01:01:01", 1i64),
            ("2018-01-01 01:01:01", 2018i64),
            ("2019-01-01 01:01:01", 2019i64),
            ("2020-01-01 01:01:01", 2020i64),
            ("2021-01-01 01:01:01", 2021i64),
            ("2024-01-01 01:01:01", 2022i64),
            ("2023-01-01 01:01:01", 2023i64),
            ("2024-01-01 01:01:01", 2024i64),
            ("2025-01-01 01:01:01", 2025i64),
            ("2026-01-01 01:01:01", 2026i64),
            ("2027-01-01 01:01:01", 2027i64),
            ("2028-01-01 01:01:01", 2028i64),
            ("2029-01-01 01:01:01", 2029i64),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Time::parse_datetime(&mut ctx, arg, 6, true).unwrap();
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::Year,
                Datum::Time(datetime),
                Datum::I64(exp),
            );
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::Year, Datum::Null);
    }

    #[test]
    fn test_week_with_mode() {
        let cases = vec![
            ("2008-02-20 00:00:00", 1, 8i64),
            ("2008-12-31 00:00:00", 1, 53i64),
            ("2000-01-01", 0, 0i64),
            ("2008-02-20", 0, 7i64),
            ("2017-01-01", 0, 1i64),
            ("2017-01-01", 1, 0i64),
            ("2017-01-01", 2, 1i64),
            ("2017-01-01", 3, 52i64),
            ("2017-01-01", 4, 1i64),
            ("2017-01-01", 5, 0i64),
            ("2017-01-01", 6, 1i64),
            ("2017-01-01", 7, 52i64),
            ("2017-12-31", 0, 53i64),
            ("2017-12-31", 1, 52i64),
            ("2017-12-31", 2, 53i64),
            ("2017-12-31", 3, 52i64),
            ("2017-12-31", 4, 53i64),
            ("2017-12-31", 5, 52i64),
            ("2017-12-31", 6, 1i64),
            ("2017-12-31", 7, 52i64),
            ("2017-12-31", 14, 1i64),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            let datetime = Time::parse_datetime(&mut ctx, arg1, 6, true).unwrap();
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::WeekWithMode,
                Datum::Time(datetime),
                Datum::I64(arg2),
                Datum::I64(exp),
            );
        }
        // test NULL case
        test_err_case_two_arg(
            &mut ctx,
            ScalarFuncSig::WeekWithMode,
            Datum::Null,
            Datum::Null,
        );
    }

    #[test]
    fn test_week_without_mode() {
        let cases = vec![("2000-01-01", 0i64)];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Time::parse_datetime(&mut ctx, arg, 6, true).unwrap();
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::WeekWithoutMode,
                Datum::Time(datetime),
                Datum::I64(exp),
            );
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::WeekWithoutMode, Datum::Null);
    }

    #[test]
    fn test_week_day() {
        let cases = vec![
            ("2018-12-03", 0i64),
            ("2018-12-04", 1i64),
            ("2018-12-05", 2i64),
            ("2018-12-06", 3i64),
            ("2018-12-07", 4i64),
            ("2018-12-08", 5i64),
            ("2018-12-09", 6i64),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Time::parse_datetime(&mut ctx, arg, 6, true).unwrap();
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::WeekDay,
                Datum::Time(datetime),
                Datum::I64(exp),
            );
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::WeekDay, Datum::Null);
    }

    #[test]
    fn test_week_of_year() {
        let cases = vec![
            ("2018-01-01", 1i64),
            ("2018-02-28", 9i64),
            ("2018-06-01", 22i64),
            ("2018-07-31", 31i64),
            ("2018-11-01", 44i64),
            ("2018-12-30", 52i64),
            ("2018-12-31", 1i64),
            ("2017-01-01", 52i64),
            ("2017-12-31", 52i64),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Datum::Time(Time::parse_datetime(&mut ctx, arg, 6, true).unwrap());
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::WeekOfYear,
                datetime,
                Datum::I64(exp),
            );
        }
        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::WeekOfYear, Datum::Null);
    }

    #[test]
    fn test_year_week_with_mode() {
        let cases = vec![
            ("1987-01-01", 0, 198652),
            ("2000-01-01", 0, 199952),
            ("0000-01-01", 0, 1),
            ("0000-01-01", 1, 4294967295),
            ("0000-01-01", 2, 1),
            ("0000-01-01", 3, 4294967295),
            ("0000-01-01", 4, 1),
            ("0000-01-01", 5, 4294967295),
            ("0000-01-01", 6, 1),
            ("0000-01-01", 7, 4294967295),
            ("0000-01-01", 15, 4294967295),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            let time = Datum::Time(Time::parse_datetime(&mut ctx, arg1, 6, true).unwrap());
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::YearWeekWithMode,
                time,
                Datum::I64(arg2),
                Datum::I64(exp),
            );
        }

        // test NULL case
        test_err_case_two_arg(
            &mut ctx,
            ScalarFuncSig::YearWeekWithMode,
            Datum::Null,
            Datum::Null,
        );

        // test ZERO case
        let time =
            Datum::Time(Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 6, true).unwrap());
        test_err_case_two_arg(
            &mut ctx,
            ScalarFuncSig::YearWeekWithMode,
            time,
            Datum::I64(0),
        );
    }

    #[test]
    fn test_year_week_without_mode() {
        let cases = vec![
            ("1987-01-01", 198652),
            ("2000-01-01", 199952),
            ("0000-01-01", 1),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let time = Datum::Time(Time::parse_datetime(&mut ctx, arg, 6, true).unwrap());
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::YearWeekWithoutMode,
                time,
                Datum::I64(exp),
            );
        }

        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::YearWeekWithoutMode, Datum::Null);

        // test ZERO case
        let time =
            Datum::Time(Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 6, true).unwrap());
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::YearWeekWithoutMode, time);
    }

    #[test]
    fn test_period_add() {
        let cases = vec![
            (2, 222, 201808),
            (0, 222, 0),
            (196802, 14, 196904),
            (6901, 13, 207002),
            (7001, 13, 197102),
            (200212, 9223372036854775807, 200211),
            (9223372036854775807, 0, 27201459511),
            (9223372036854775807, 9223372036854775807, 27201459510),
            (201611, 2, 201701),
            (201611, 3, 201702),
            (201611, -13, 201510),
            (1611, 3, 201702),
            (7011, 3, 197102),
            (12323, 10, 12509),
            (0, 3, 0),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::PeriodAdd,
                Datum::I64(arg1),
                Datum::I64(arg2),
                Datum::I64(exp),
            );
        }
    }

    #[test]
    fn test_period_diff() {
        let cases = vec![
            (213002, 7010, 1912),
            (213002, 215810, -344),
            (2202, 9601, 313),
            (202202, 9601, 313),
            (200806, 6907, -733),
            (201611, 201611, 0),
            (200802, 200703, 11),
            (0, 999999999, -120000086),
            (9999999, 0, 1200086),
            (411, 200413, -2),
            (197000, 207700, -1284),
            (201701, 201611, 2),
            (201702, 201611, 3),
            (201510, 201611, -13),
            (201702, 1611, 3),
            (197102, 7011, 3),
            (12509, 12323, 10),
            (12509, 12323, 10),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::PeriodDiff,
                Datum::I64(arg1),
                Datum::I64(arg2),
                Datum::I64(exp),
            );
        }
    }

    #[test]
    fn test_to_days() {
        let cases = vec![
            ("950501", 728779),
            ("2007-10-07", 733321),
            ("2008-10-07", 733687),
            ("08-10-07", 733687),
            ("0000-01-01", 1),
            ("2007-10-07 00:00:59", 733321),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let time = Datum::Time(Time::parse_datetime(&mut ctx, arg, 6, true).unwrap());
            test_ok_case_one_arg(&mut ctx, ScalarFuncSig::ToDays, time, Datum::I64(exp));
        }

        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::ToDays, Datum::Null);

        let datetime =
            Datum::Time(Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 6, true).unwrap());
        // test ZERO case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::ToDays, datetime);
    }

    #[test]
    fn test_to_seconds() {
        let cases = vec![
            ("950501", 62966505600),
            ("2009-11-29", 63426672000),
            ("2009-11-29 13:43:32", 63426721412),
            ("09-11-29 13:43:32", 63426721412),
            ("99-11-29 13:43:32", 63111102212),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let time = Datum::Time(Time::parse_datetime(&mut ctx, arg, 6, true).unwrap());
            test_ok_case_one_arg(&mut ctx, ScalarFuncSig::ToSeconds, time, Datum::I64(exp));
        }

        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::ToSeconds, Datum::Null);

        let datetime =
            Datum::Time(Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 6, true).unwrap());
        // test ZERO case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::ToSeconds, datetime);
    }

    #[test]
    fn test_date_diff() {
        let cases = vec![
            (
                "0000-01-01 00:00:00.000000",
                "0000-01-01 00:00:00.000000",
                0,
            ),
            (
                "2018-02-01 00:00:00.000000",
                "2018-02-01 00:00:00.000000",
                0,
            ),
            (
                "2018-02-02 00:00:00.000000",
                "2018-02-01 00:00:00.000000",
                1,
            ),
            (
                "2018-02-01 00:00:00.000000",
                "2018-02-02 00:00:00.000000",
                -1,
            ),
            (
                "2018-02-02 00:00:00.000000",
                "2018-02-01 23:59:59.999999",
                1,
            ),
            (
                "2018-02-01 23:59:59.999999",
                "2018-02-02 00:00:00.000000",
                -1,
            ),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            let arg1 = Datum::Time(Time::parse_datetime(&mut ctx, arg1, 6, true).unwrap());
            let arg2 = Datum::Time(Time::parse_datetime(&mut ctx, arg2, 6, true).unwrap());
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::DateDiff,
                arg1,
                arg2,
                Datum::I64(exp),
            );
        }

        let mut causet = EvalConfig::new();
        causet.set_flag(Flag::IN_fidelio_OR_DELETE_STMT)
            .set_sql_mode(SqlMode::ERROR_FOR_DIVISION_BY_ZERO | SqlMode::STRICT_ALL_BlockS);

        test_err_case_two_arg(&mut ctx, ScalarFuncSig::DateDiff, Datum::Null, Datum::Null);
    }

    #[test]
    fn test_add_sub_datetime_and_duration() {
        let cases = vec![
            (
                "2018-01-01",
                "11:30:45.123456",
                "2018-01-01 11:30:45.123456",
            ),
            (
                "2018-02-28 23:00:00",
                "01:30:30.123456",
                "2018-03-01 00:30:30.123456",
            ),
            ("2016-02-28 23:00:00", "01:30:30", "2016-02-29 00:30:30"),
            ("2018-12-31 23:00:00", "01:30:30", "2019-01-01 00:30:30"),
            ("2018-12-31 23:00:00", "1 01:30:30", "2019-01-02 00:30:30"),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            {
                let arg1 = Datum::Time(Time::parse_datetime(&mut ctx, arg1, 6, true).unwrap());
                let exp = Datum::Time(Time::parse_datetime(&mut ctx, exp, 6, true).unwrap());
                test_ok_case_two_arg(
                    &mut ctx,
                    ScalarFuncSig::AddDatetimeAndDuration,
                    arg1,
                    Datum::Dur(
                        Duration::parse(&mut EvalContext::default(), arg2.as_bytes(), 6).unwrap(),
                    ),
                    exp,
                );
            }
            let exp = Datum::Time(Time::parse_datetime(&mut ctx, exp, 6, true).unwrap());
            let arg1 = Datum::Time(Time::parse_datetime(&mut ctx, arg1, 6, true).unwrap());
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::SubDatetimeAndDuration,
                exp,
                Datum::Dur(
                    Duration::parse(&mut EvalContext::default(), arg2.as_bytes(), 6).unwrap(),
                ),
                arg1,
            );
        }

        let cases = vec![
            (
                Datum::Null,
                Datum::Dur(Duration::parse(&mut ctx, b"11:30:45.123456", 6).unwrap()),
                Datum::Null,
            ),
            (Datum::Null, Datum::Null, Datum::Null),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2019-01-01 01:00:00", 6, true).unwrap(),
                ),
                Datum::Dur(Duration::zero()),
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2019-01-01 01:00:00", 6, true).unwrap(),
                ),
            ),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2019-01-01 01:00:00", 6, true).unwrap(),
                ),
                Datum::Dur(Duration::parse(&mut ctx, b"-01:01:00", 6).unwrap()),
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2018-12-31 23:59:00", 6, true).unwrap(),
                ),
            ),
        ];
        for (arg1, arg2, exp) in cases {
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::AddDatetimeAndDuration,
                arg1.clone(),
                arg2.clone(),
                exp.clone(),
            );
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::SubDatetimeAndDuration,
                exp,
                arg2,
                arg1,
            );
        }
    }

    #[test]
    fn test_add_sub_datetime_and_string() {
        let cases = vec![
            (
                "2018-01-01",
                "11:30:45.123456",
                "2018-01-01 11:30:45.123456",
            ),
            (
                "2018-02-28 23:00:00",
                "01:30:30.123456",
                "2018-03-01 00:30:30.123456",
            ),
            ("2016-02-28 23:00:00", "01:30:30", "2016-02-29 00:30:30"),
            ("2018-12-31 23:00:00", "01:30:30", "2019-01-01 00:30:30"),
            ("2018-12-31 23:00:00", "1 01:30:30", "2019-01-02 00:30:30"),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            {
                let arg1 = Datum::Time(Time::parse_datetime(&mut ctx, arg1, 6, true).unwrap());
                let exp = Datum::Time(Time::parse_datetime(&mut ctx, exp, 6, true).unwrap());
                test_ok_case_two_arg(
                    &mut ctx,
                    ScalarFuncSig::AddDatetimeAndString,
                    arg1,
                    Datum::Bytes(arg2.as_bytes().to_vec()),
                    exp,
                );
            }

            let exp = Datum::Time(Time::parse_datetime(&mut ctx, exp, 6, true).unwrap());
            let arg1 = Datum::Time(Time::parse_datetime(&mut ctx, arg1, 6, true).unwrap());
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::SubDatetimeAndString,
                exp,
                Datum::Bytes(arg2.as_bytes().to_vec()),
                arg1,
            );
        }

        let cases = vec![
            (
                Datum::Null,
                Datum::Dur(
                    Duration::parse(&mut EvalContext::default(), b"11:30:45.123456", 6).unwrap(),
                ),
                Datum::Null,
            ),
            (Datum::Null, Datum::Null, Datum::Null),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2019-01-01 01:00:00", 6, true).unwrap(),
                ),
                Datum::Bytes(b"00:00:00".to_vec()),
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2019-01-01 01:00:00", 6, true).unwrap(),
                ),
            ),
            (
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2019-01-01 01:00:00", 6, true).unwrap(),
                ),
                Datum::Bytes(b"-01:01:00".to_vec()),
                Datum::Time(
                    Time::parse_datetime(&mut ctx, "2018-12-31 23:59:00", 6, true).unwrap(),
                ),
            ),
        ];
        for (arg1, arg2, exp) in cases {
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::AddDatetimeAndString,
                arg1.clone(),
                arg2.clone(),
                exp.clone(),
            );
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::SubDatetimeAndString,
                exp,
                arg2,
                arg1,
            );
        }

        let datetime =
            Datum::Time(Time::parse_datetime(&mut ctx, "2019-01-01 01:00:00", 6, true).unwrap());
        test_ok_case_two_arg(
            &mut ctx,
            ScalarFuncSig::AddDatetimeAndString,
            datetime.clone(),
            Datum::Bytes(b"xxx".to_vec()),
            Datum::Null,
        );
        test_ok_case_two_arg(
            &mut ctx,
            ScalarFuncSig::SubDatetimeAndString,
            datetime,
            Datum::Bytes(b"xxx".to_vec()),
            Datum::Null,
        );
    }

    #[test]
    fn test_add_sub_time_datetime_null() {
        let mut ctx = EvalContext::default();
        test_ok_case_zero_arg(&mut ctx, ScalarFuncSig::AddTimeDateTimeNull, Datum::Null);
        test_ok_case_zero_arg(&mut ctx, ScalarFuncSig::SubTimeDateTimeNull, Datum::Null);
    }

    #[test]
    fn test_add_sub_duration_and_duration() {
        let cases = vec![
            ("01:00:00.999999", "02:00:00.999998", "03:00:01.999997"),
            ("23:59:59", "00:00:01", "24:00:00"),
            ("235959", "00:00:01", "24:00:00"),
            ("110:00:00", "1 02:00:00", "136:00:00"),
            ("-110:00:00", "1 02:00:00", "-84:00:00"),
            ("00:00:01", "-00:00:01", "00:00:00"),
            ("00:00:03", "-00:00:01", "00:00:02"),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::AddDurationAndDuration,
                Datum::Dur(Duration::parse(&mut EvalContext::default(), arg1.as_ref(), 6).unwrap()),
                Datum::Dur(Duration::parse(&mut EvalContext::default(), arg2.as_ref(), 6).unwrap()),
                Datum::Dur(Duration::parse(&mut EvalContext::default(), exp.as_ref(), 6).unwrap()),
            );
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::SubDurationAndDuration,
                Datum::Dur(Duration::parse(&mut EvalContext::default(), exp.as_ref(), 6).unwrap()),
                Datum::Dur(Duration::parse(&mut EvalContext::default(), arg2.as_ref(), 6).unwrap()),
                Datum::Dur(Duration::parse(&mut EvalContext::default(), arg1.as_ref(), 6).unwrap()),
            );
        }

        let zero_duration = Datum::Dur(Duration::zero());
        let cases = vec![
            (
                Datum::Null,
                Datum::Dur(Duration::parse(&mut ctx, b"11:30:45.123456", 6).unwrap()),
                Datum::Null,
            ),
            (Datum::Null, Datum::Null, Datum::Null),
            (
                zero_duration.clone(),
                zero_duration.clone(),
                zero_duration.clone(),
            ),
            (
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
                zero_duration.clone(),
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
            ),
            (
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
                Datum::Dur(Duration::parse(&mut ctx, b"-01:00:00", 6).unwrap()),
                zero_duration,
            ),
        ];
        for (arg1, arg2, exp) in cases {
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::AddDurationAndDuration,
                arg1.clone(),
                arg2.clone(),
                exp.clone(),
            );
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::SubDurationAndDuration,
                exp,
                arg2,
                arg1,
            );
        }
    }

    #[test]
    fn test_add_sub_duration_and_string() {
        let cases = vec![
            ("01:00:00.999999", "02:00:00.999998", "03:00:01.999997"),
            ("23:59:59", "00:00:01", "24:00:00"),
            ("235959", "00:00:01", "24:00:00"),
            ("110:00:00", "1 02:00:00", "136:00:00"),
            ("-110:00:00", "1 02:00:00", "-84:00:00"),
            ("00:00:01", "-00:00:01", "00:00:00"),
            ("00:00:03", "-00:00:01", "00:00:02"),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::AddDurationAndString,
                Datum::Dur(Duration::parse(&mut EvalContext::default(), arg1.as_ref(), 6).unwrap()),
                Datum::Bytes(arg2.as_bytes().to_vec()),
                Datum::Dur(Duration::parse(&mut EvalContext::default(), exp.as_ref(), 6).unwrap()),
            );
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::SubDurationAndString,
                Datum::Dur(Duration::parse(&mut EvalContext::default(), exp.as_ref(), 6).unwrap()),
                Datum::Bytes(arg2.as_bytes().to_vec()),
                Datum::Dur(Duration::parse(&mut EvalContext::default(), arg1.as_ref(), 6).unwrap()),
            );
        }

        let zero_duration = Datum::Dur(Duration::zero());
        let zero_duration_string = Datum::Bytes(b"00:00:00".to_vec());
        let cases = vec![
            (
                Datum::Null,
                Datum::Bytes(b"11:30:45.123456".to_vec()),
                Datum::Null,
            ),
            (Datum::Null, Datum::Null, Datum::Null),
            (
                zero_duration.clone(),
                zero_duration_string.clone(),
                zero_duration.clone(),
            ),
            (
                zero_duration.clone(),
                Datum::Bytes(b"01:00:00".to_vec()),
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
            ),
            (
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
                zero_duration_string,
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
            ),
            (
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
                Datum::Bytes(b"-01:00:00".to_vec()),
                zero_duration,
            ),
        ];
        for (arg1, arg2, exp) in cases {
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::AddDurationAndString,
                arg1.clone(),
                arg2.clone(),
                exp.clone(),
            );
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::SubDurationAndString,
                exp,
                arg2,
                arg1,
            );
        }

        test_ok_case_two_arg(
            &mut ctx,
            ScalarFuncSig::AddDurationAndString,
            Datum::Dur(Duration::parse(&mut EvalContext::default(), b"01:00:00", 6).unwrap()),
            Datum::Bytes(b"xxx".to_vec()),
            Datum::Null,
        );
        test_ok_case_two_arg(
            &mut ctx,
            ScalarFuncSig::SubDurationAndString,
            Datum::Dur(Duration::parse(&mut EvalContext::default(), b"01:00:00", 6).unwrap()),
            Datum::Bytes(b"xxx".to_vec()),
            Datum::Null,
        );
    }

    #[test]
    fn test_add_sub_time_duration_null() {
        let mut ctx = EvalContext::default();
        test_ok_case_zero_arg(&mut ctx, ScalarFuncSig::AddTimeDurationNull, Datum::Null);
        test_ok_case_zero_arg(&mut ctx, ScalarFuncSig::SubTimeDurationNull, Datum::Null);
    }

    #[test]
    fn test_add_sub_date_and_duration() {
        let cases = vec![
            ("01:00:00.999999", "02:00:00.999998", "03:00:01.999997"),
            ("23:59:59.000000", "00:00:01", "24:00:00.000000"),
            ("110:00:00.000000", "1 02:00:00", "136:00:00.000000"),
            ("-110:00:00.000000", "1 02:00:00", "-84:00:00.000000"),
            ("00:00:01.000000", "-00:00:01", "00:00:00.000000"),
            ("00:00:03.000000", "-00:00:01", "00:00:02.000000"),
        ];

        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::AddDateAndDuration,
                Datum::Dur(Duration::parse(&mut EvalContext::default(), arg1.as_ref(), 6).unwrap()),
                Datum::Dur(Duration::parse(&mut EvalContext::default(), arg2.as_ref(), 6).unwrap()),
                Datum::Bytes(exp.as_bytes().to_vec()),
            );
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::SubDateAndDuration,
                Datum::Dur(Duration::parse(&mut EvalContext::default(), exp.as_ref(), 6).unwrap()),
                Datum::Dur(Duration::parse(&mut EvalContext::default(), arg2.as_ref(), 6).unwrap()),
                Datum::Bytes(arg1.as_bytes().to_vec()),
            );
        }

        // ZERO & NULL case test
        let zero_duration = Datum::Dur(Duration::zero());
        let zero_duration_string = Datum::Bytes(b"00:00:00.000000".to_vec());
        let cases = vec![
            (
                Datum::Null,
                Datum::Dur(Duration::parse(&mut ctx, b"11:30:45.123456", 6).unwrap()),
                Datum::Null,
            ),
            (Datum::Null, Datum::Null, Datum::Null),
            (
                zero_duration.clone(),
                zero_duration.clone(),
                zero_duration_string.clone(),
            ),
            (
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
                zero_duration,
                Datum::Bytes(b"01:00:00.000000".to_vec()),
            ),
            (
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
                Datum::Dur(Duration::parse(&mut ctx, b"-01:00:00", 6).unwrap()),
                zero_duration_string,
            ),
        ];
        for (arg1, arg2, exp) in cases {
            test_ok_case_two_arg(&mut ctx, ScalarFuncSig::AddDateAndDuration, arg1, arg2, exp);
        }

        let zero_duration = Datum::Dur(Duration::zero());
        let zero_duration_string = Datum::Bytes(b"00:00:00.000000".to_vec());
        let cases = vec![
            (
                Datum::Null,
                Datum::Dur(Duration::parse(&mut ctx, b"11:30:45.123456", 6).unwrap()),
                Datum::Null,
            ),
            (Datum::Null, Datum::Null, Datum::Null),
            (
                zero_duration.clone(),
                zero_duration.clone(),
                zero_duration_string.clone(),
            ),
            (
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
                zero_duration,
                Datum::Bytes(b"01:00:00.000000".to_vec()),
            ),
            (
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
                zero_duration_string,
            ),
        ];
        for (arg1, arg2, exp) in cases {
            test_ok_case_two_arg(&mut ctx, ScalarFuncSig::SubDateAndDuration, arg1, arg2, exp);
        }
    }

    #[test]
    fn test_add_sub_date_and_string() {
        let cases = vec![
            ("01:00:00.999999", "02:00:00.999998", "03:00:01.999997"),
            ("23:59:59.000000", "00:00:01", "24:00:00.000000"),
            ("110:00:00.000000", "1 02:00:00", "136:00:00.000000"),
            ("-110:00:00.000000", "1 02:00:00", "-84:00:00.000000"),
            ("00:00:01.000000", "-00:00:01", "00:00:00.000000"),
            ("00:00:03.000000", "-00:00:01", "00:00:02.000000"),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::AddDateAndString,
                Datum::Dur(Duration::parse(&mut EvalContext::default(), arg1.as_ref(), 6).unwrap()),
                Datum::Bytes(arg2.as_bytes().to_vec()),
                Datum::Bytes(exp.as_bytes().to_vec()),
            );
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::SubDateAndString,
                Datum::Dur(Duration::parse(&mut EvalContext::default(), exp.as_ref(), 6).unwrap()),
                Datum::Bytes(arg2.as_bytes().to_vec()),
                Datum::Bytes(arg1.as_bytes().to_vec()),
            );
        }

        // ZERO & NULL case test
        let zero_duration = Datum::Dur(Duration::zero());
        let zero_duration_string = Datum::Bytes(b"00:00:00.000000".to_vec());
        let cases = vec![
            (
                Datum::Null,
                Datum::Bytes(b"11:30:45.123456".to_vec()),
                Datum::Null,
            ),
            (Datum::Null, Datum::Null, Datum::Null),
            (
                zero_duration.clone(),
                zero_duration_string.clone(),
                zero_duration_string.clone(),
            ),
            (
                zero_duration,
                Datum::Bytes(b"01:00:00".to_vec()),
                Datum::Bytes(b"01:00:00.000000".to_vec()),
            ),
            (
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
                zero_duration_string.clone(),
                Datum::Bytes(b"01:00:00.000000".to_vec()),
            ),
            (
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
                Datum::Bytes(b"-01:00:00".to_vec()),
                zero_duration_string,
            ),
        ];
        for (arg1, arg2, exp) in cases {
            test_ok_case_two_arg(&mut ctx, ScalarFuncSig::AddDateAndString, arg1, arg2, exp);
        }

        let zero_duration = Datum::Dur(Duration::zero());
        let zero_duration_string = Datum::Bytes(b"00:00:00.000000".to_vec());
        let cases = vec![
            (
                Datum::Null,
                Datum::Bytes(b"11:30:45.123456".to_vec()),
                Datum::Null,
            ),
            (Datum::Null, Datum::Null, Datum::Null),
            (
                zero_duration.clone(),
                zero_duration_string.clone(),
                zero_duration_string.clone(),
            ),
            (
                zero_duration,
                Datum::Bytes(b"01:00:00".to_vec()),
                Datum::Bytes(b"-01:00:00.000000".to_vec()),
            ),
            (
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
                zero_duration_string.clone(),
                Datum::Bytes(b"01:00:00.000000".to_vec()),
            ),
            (
                Datum::Dur(Duration::parse(&mut ctx, b"01:00:00", 6).unwrap()),
                Datum::Bytes(b"01:00:00".to_vec()),
                zero_duration_string,
            ),
        ];
        for (arg1, arg2, exp) in cases {
            test_ok_case_two_arg(&mut ctx, ScalarFuncSig::SubDateAndString, arg1, arg2, exp);
        }

        // invalid string test
        test_ok_case_two_arg(
            &mut ctx,
            ScalarFuncSig::AddDateAndString,
            Datum::Dur(Duration::parse(&mut EvalContext::default(), b"01:00:00", 6).unwrap()),
            Datum::Bytes(b"xxx".to_vec()),
            Datum::Null,
        );
        test_ok_case_two_arg(
            &mut ctx,
            ScalarFuncSig::SubDateAndString,
            Datum::Dur(Duration::parse(&mut EvalContext::default(), b"01:00:00", 6).unwrap()),
            Datum::Bytes(b"xxx".to_vec()),
            Datum::Null,
        );
    }

    #[test]
    fn test_add_sub_time_string_null() {
        let mut ctx = EvalContext::default();
        test_ok_case_zero_arg(&mut ctx, ScalarFuncSig::AddTimeStringNull, Datum::Null);
    }

    #[test]
    fn test_from_days() {
        let cases = vec![
            (-140, "0000-00-00"), // mysql FROM_DAYS returns 0000-00-00 for any day <= 365.
            (140, "0000-00-00"),  // mysql FROM_DAYS returns 0000-00-00 for any day <= 365.
            (735_000, "2012-05-12"), // Leap year.
            (735_030, "2012-06-11"),
            (735_130, "2012-09-19"),
            (734_909, "2012-02-11"),
            (734_878, "2012-01-11"),
            (734_927, "2012-02-29"),
            (734_634, "2011-05-12"), // Non Leap year.
            (734_664, "2011-06-11"),
            (734_764, "2011-09-19"),
            (734_544, "2011-02-11"),
            (734_513, "2011-01-11"),
            (3_652_424, "9999-12-31"),
            (3_652_425, "0000-00-00"), // mysql FROM_DAYS returns 0000-00-00 for any day >= 3652425
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let datetime = Time::parse_date(&mut ctx, exp).unwrap();
            test_ok_case_one_arg(
                &mut ctx,
                ScalarFuncSig::FromDays,
                Datum::I64(arg),
                Datum::Time(datetime),
            );
        }

        // test NULL case
        test_err_case_one_arg(&mut ctx, ScalarFuncSig::Month, Datum::Null);
    }

    #[test]
    fn test_make_date() {
        let null_cases = vec![
            (Datum::Null, Datum::Null),
            (Datum::I64(2014), Datum::I64(0)),
            (Datum::I64(10000), Datum::I64(1)),
            (Datum::I64(9999), Datum::I64(366)),
            (Datum::I64(-1), Datum::I64(1)),
            (Datum::I64(-4294965282), Datum::I64(1)),
            (Datum::I64(0), Datum::I64(0)),
            (Datum::I64(0), Datum::I64(-1)),
            (Datum::I64(10), Datum::I64(-1)),
            (Datum::I64(0), Datum::I64(9223372036854775807)),
            (Datum::I64(100), Datum::I64(9999 * 366)),
            (Datum::I64(9999), Datum::I64(9999 * 366)),
            (Datum::I64(100), Datum::I64(3615901)),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2) in null_cases {
            test_err_case_two_arg(&mut ctx, ScalarFuncSig::MakeDate, arg1, arg2);
        }
        let cases = vec![
            (0, 1, "2000-01-01"),
            (70, 1, "1970-01-01"),
            (71, 1, "1971-01-01"),
            (99, 1, "1999-01-01"),
            (100, 1, "0100-01-01"),
            (101, 1, "0101-01-01"),
            (2014, 224234, "2627-12-07"),
            (2014, 1, "2014-01-01"),
            (7900, 705000, "9830-03-23"),
            (7901, 705000, "9831-03-23"),
            (7904, 705000, "9834-03-22"),
            (8000, 705000, "9930-03-23"),
            (8001, 705000, "9931-03-24"),
            (100, 3615900, "9999-12-31"),
        ];
        for (arg1, arg2, exp) in cases {
            let datetime = Time::parse_date(&mut ctx, exp).unwrap();
            test_ok_case_two_arg(
                &mut ctx,
                ScalarFuncSig::MakeDate,
                Datum::I64(arg1),
                Datum::I64(arg2),
                Datum::Time(datetime),
            );
        }
    }
}
