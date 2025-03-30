
use chrono::{DateTime, Duration, NaiveDate, NaiveTime};
use std::path::Path;

pub fn file_exists(filename: &str) -> bool {
    let path = Path::new(filename);
    path.is_file()
}


/// Extracts the file extension from a given filename or path.
///
/// This function takes a string representing a filename or path and returns
/// the lowercase extension without the dot, or an empty string if no extension is found.
///
/// # Arguments
///
/// * `filename` - A string slice containing the filename or path
///
/// # Returns
///
/// * `String` - The lowercase file extension without the dot, or an empty string if no extension is found
///
/// # Examples
///
/// ```
/// let ext = get_file_extension("data.parquet");
/// assert_eq!(ext, "parquet");
///
/// let ext = get_file_extension("/path/to/file.CSV");
/// assert_eq!(ext, "csv");
///
/// let ext = get_file_extension("no_extension");
/// assert_eq!(ext, "");
/// ```
pub fn get_file_extension(filename: &str) -> String {
    filename
        .rsplit('.')
        .next()
        .and_then(|ext| {
            if filename.ends_with('.') || ext == filename {
                None
            } else {
                Some(ext.to_lowercase())
            }
        })
        .unwrap_or_default()
}

pub fn date32_to_ymd(date32: i32) -> String {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date = epoch + Duration::days(date32 as i64);
    date.format("%Y-%m-%d").to_string()
}

pub fn timeunit_to_ymd_hms(unit: duckdb::types::TimeUnit, i64timestamp: i64) -> String {
    match unit {
        duckdb::types::TimeUnit::Second => {
            let datetime = DateTime::from_timestamp(i64timestamp, 0);
            match datetime {
                Some(dt) => dt.format("%Y-%m-%dT%H:%M:%S%:z").to_string(),
                None => "Invalid Time".to_string(),
            }
        }
        duckdb::types::TimeUnit::Millisecond => {
            let seconds = i64timestamp / 1_000;
            let nanoseconds = (i64timestamp % 1_000) * 1_000_000;
            let datetime = DateTime::from_timestamp(seconds, nanoseconds as u32);
            match datetime {
                Some(dt) => dt.format("%Y-%m-%dT%H:%M:%S.%3f%:z").to_string(),
                None => "Invalid Time".to_string(),
            }
        }
        duckdb::types::TimeUnit::Microsecond => {
            let seconds = i64timestamp / 1_000_000;
            let nanoseconds = (i64timestamp % 1_000_000) * 1_000;
            let datetime = DateTime::from_timestamp(seconds, nanoseconds as u32);
            match datetime {
                Some(dt) => dt.format("%Y-%m-%dT%H:%M:%S.%6f%:z").to_string(),
                None => "Invalid Time".to_string(),
            }
        }
        duckdb::types::TimeUnit::Nanosecond => {
            let seconds = i64timestamp / 1_000_000_000;
            let nanoseconds = (i64timestamp % 1_000_000_000) as u32;
            let datetime = DateTime::from_timestamp(seconds, nanoseconds);
            match datetime {
                Some(dt) => dt.format("%Y-%m-%dT%H:%M:%S.%9f%:z").to_string(),
                None => "Invalid Time".to_string(),
            }
        }
    }
}

pub fn timeunit_to_hms(unit: duckdb::types::TimeUnit, i64timestamp: i64) -> String {
    match unit {
        duckdb::types::TimeUnit::Second => {
            let time = NaiveTime::from_num_seconds_from_midnight_opt(i64timestamp as u32, 0);
            match time {
                Some(t) => t.format("%H:%M:%S").to_string(),
                None => "Invalid Time".to_string(),
            }
        }
        duckdb::types::TimeUnit::Millisecond => {
            let seconds = i64timestamp / 1_000;
            let nanoseconds = (i64timestamp % 1_000) * 1_000_000; // Convert remaining milliseconds to nanoseconds
            let time =
                NaiveTime::from_num_seconds_from_midnight_opt(seconds as u32, nanoseconds as u32);
            match time {
                Some(t) => t.format("%H:%M:%S.%3f").to_string(),
                None => "Invalid Time".to_string(),
            }
        }
        duckdb::types::TimeUnit::Microsecond => {
            let seconds = i64timestamp / 1_000_000;
            let nanoseconds = (i64timestamp % 1_000_000) * 1_000; // Convert remaining microseconds to nanoseconds
            let time =
                NaiveTime::from_num_seconds_from_midnight_opt(seconds as u32, nanoseconds as u32);
            match time {
                Some(t) => t.format("%H:%M:%S.%6f").to_string(),
                None => "Invalid Time".to_string(),
            }
        }
        duckdb::types::TimeUnit::Nanosecond => {
            let seconds = i64timestamp / 1_000_000_000;
            let nanoseconds = (i64timestamp % 1_000_000_000) as u32; // Use the remainder as nanoseconds
            let time = NaiveTime::from_num_seconds_from_midnight_opt(seconds as u32, nanoseconds);
            match time {
                Some(t) => t.format("%H:%M:%S.%9f").to_string(),
                None => "Invalid Time".to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::types::TimeUnit;

    #[test]
    fn test_date32_to_ymd() {
        assert_eq!(date32_to_ymd(19275), "2022-10-10");
        assert_eq!(date32_to_ymd(0), "1970-01-01"); // Unix epoch
        assert_eq!(date32_to_ymd(-1), "1969-12-31"); // Day before Unix epoch
    }

    #[test]
    fn test_timeunit_to_hms_seconds() {
        assert_eq!(timeunit_to_hms(TimeUnit::Second, 3661), "01:01:01");
        assert_eq!(timeunit_to_hms(TimeUnit::Second, 0), "00:00:00");
        assert_eq!(timeunit_to_hms(TimeUnit::Second, 86399), "23:59:59");
    }

    #[test]
    fn test_timeunit_to_hms_milliseconds() {
        assert_eq!(
            timeunit_to_hms(TimeUnit::Millisecond, 3661000),
            "01:01:01.000"
        );
        assert_eq!(timeunit_to_hms(TimeUnit::Millisecond, 0), "00:00:00.000");
        assert_eq!(
            timeunit_to_hms(TimeUnit::Millisecond, 86399999),
            "23:59:59.999"
        );
    }

    #[test]
    fn test_timeunit_to_hms_microseconds() {
        assert_eq!(
            timeunit_to_hms(TimeUnit::Microsecond, 3661000000),
            "01:01:01.000000"
        );
        assert_eq!(timeunit_to_hms(TimeUnit::Microsecond, 0), "00:00:00.000000");
        assert_eq!(
            timeunit_to_hms(TimeUnit::Microsecond, 86399999999),
            "23:59:59.999999"
        );
    }

    #[test]
    fn test_timeunit_to_hms_nanoseconds() {
        assert_eq!(
            timeunit_to_hms(TimeUnit::Nanosecond, 3661000000000),
            "01:01:01.000000000"
        );
        assert_eq!(
            timeunit_to_hms(TimeUnit::Nanosecond, 0),
            "00:00:00.000000000"
        );
        assert_eq!(
            timeunit_to_hms(TimeUnit::Nanosecond, 86399999999999),
            "23:59:59.999999999"
        );
    }

    #[test]
    fn test_timeunit_to_ymd_hms_seconds() {
        assert_eq!(
            timeunit_to_ymd_hms(TimeUnit::Second, 1_614_764_661), // Equivalent to 2021-03-07T06:11:01+00:00
            "2021-03-03T09:44:21+00:00"
        );
        assert_eq!(
            timeunit_to_ymd_hms(TimeUnit::Second, 0), // Unix epoch
            "1970-01-01T00:00:00+00:00"
        );
    }

    #[test]
    fn test_timeunit_to_ymd_hms_milliseconds() {
        assert_eq!(
            timeunit_to_ymd_hms(TimeUnit::Millisecond, 1_614_764_661_000), // Equivalent to 2021-03-07T06:11:01.000+00:00
            "2021-03-03T09:44:21.000+00:00"
        );
        assert_eq!(
            timeunit_to_ymd_hms(TimeUnit::Millisecond, 0), // Unix epoch
            "1970-01-01T00:00:00.000+00:00"
        );
    }

    #[test]
    fn test_timeunit_to_ymd_hms_microseconds() {
        assert_eq!(
            timeunit_to_ymd_hms(TimeUnit::Microsecond, 1_614_764_661_000_000), // Equivalent to 2021-03-07T06:11:01.000000+00:00
            "2021-03-03T09:44:21.000000+00:00"
        );
        assert_eq!(
            timeunit_to_ymd_hms(TimeUnit::Microsecond, 0), // Unix epoch
            "1970-01-01T00:00:00.000000+00:00"
        );
    }

    #[test]
    fn test_timeunit_to_ymd_hms_nanoseconds() {
        assert_eq!(
            timeunit_to_ymd_hms(TimeUnit::Nanosecond, 1_614_764_661_000_000_000), // Equivalent to 2021-03-07T06:11:01.000000000+00:00
            "2021-03-03T09:44:21.000000000+00:00"
        );
        assert_eq!(
            timeunit_to_ymd_hms(TimeUnit::Nanosecond, 0), // Unix epoch
            "1970-01-01T00:00:00.000000000+00:00"
        );
    }
}