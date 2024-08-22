use std::cmp::Ordering;

use crate::{Calendar};

// Original C Source
// int32 calculate_page_size(int32 first_date, int32 last_date, int32 entry_count) {
//     int32 date_range = last_date - first_date;
//     double avg_entries_per_week_calendar = date_range / 7.0;
//     double entry_count_d = (double)entry_count;
//
//     int32 page_size_tmp = 32; // monthly calendar
//
//     if (entry_count_d > avg_entries_per_week_calendar) {
//         page_size_tmp = 16; // weekly calendar
//     }
//
//     return page_size_tmp;
// }
pub fn calculate_page_size(first_date: i32, last_date: i32, entry_count: i64) -> i32 {
    let avg_entries_per_week_calendar = (last_date - first_date) as f64 / 7.0;

    if entry_count as f64 > avg_entries_per_week_calendar {
        16 // weekly
    } else {
        32 // monthly
    }
}

// Original C Source
// int32 left_binary_search(const int32 *arr, int32 left, int32 right, int32 value) {
//     while (left <= right) {
//         int32 mid = left + (right - left) / 2;
//         if (arr[mid] < value)
//             left = mid + 1;
//         else if (arr[mid] > value) {
//             right = mid - 1;
//         } else
//             return mid;
//     }
//     return left - 1;
// }
fn left_binary_search(arr: &[i32], mut left: i32, mut right: i32, value: i32) -> i32 {
    while left <= right {
        let mid = left + (right - left) / 2;
        match arr[mid as usize].cmp(&value) {
            Ordering::Less => left = mid + 1,
            Ordering::Greater => right = mid - 1,
            Ordering::Equal => return mid,
        }
    }
    left - 1
}

// Original C Source
// int32 get_closest_index_from_left(int32 date_adt, Calendar calendar) {
//     int32 page_map_index = (date_adt / calendar.page_size) - calendar.first_page_offset;
//
//     if (page_map_index >= calendar.page_map_size) {
//         int32 ret_val = -1 * calendar.dates_size - 1;
//         return ret_val;
//     } else if (page_map_index < 0) {
//         int32 ret_val = -1;
//         return ret_val;
//     }
//
//     int32 inclusive_start_index = calendar.page_map[page_map_index];
//     int32 exclusive_end_index =
//         page_map_index < calendar.page_map_size - 1 ?
//     calendar.page_map[page_map_index + 1] :
//     calendar.dates_size;
//
//     return left_binary_search(calendar.dates,
//         inclusive_start_index,
//         exclusive_end_index,
//         date_adt);
// }
pub fn get_closest_index_from_left(date: i32, calendar: &Calendar) -> i32 {
    let page_map_index = (date / calendar.page_size) - calendar.first_page_offset;

    // debug1!("page_map_index: {}, date: {}, calendar.page_size: {}, calendar.first_page_offset: {}",
    //     page_map_index, date, calendar.page_size, calendar.first_page_offset);

    if page_map_index >= calendar.page_map.len() as i32 {
        return -(calendar.dates.len() as i32) - 1;
    } else if page_map_index < 0 {
        return -1;
    }

    let inclusive_start_index = calendar.page_map[page_map_index as usize];
    let exclusive_end_index = if page_map_index < calendar.page_map.len() as i32 - 1 {
        calendar.page_map[page_map_index as usize + 1]
    } else {
        calendar.dates.len()
    };

    // debug1!("get_closest_index_from_left: inclusive_start_index: {}, exclusive_end_index: {}", inclusive_start_index, exclusive_end_index);

    left_binary_search(
        &calendar.dates,
        inclusive_start_index as i32,
        (exclusive_end_index - 1) as i32,
        date,
    )
}

// Original C Source
// int32 add_calendar_days(
//     const IMCX *imcx,
//     const Calendar *calendar,
//     int32 input_date,
//     int32 interval,
//     int32 *result_date,
//     int32 *first_date_idx,
//     int32 *result_date_idx
// ) {
//     if (!imcx->cache_filled) {
//         return RET_ERROR_NOT_READY;
//     }
//     // Find the interval -> the closest date index from the left of the input_date in the calendar
//     int32 prev_date_index = get_closest_index_from_left(input_date, *calendar);
//     // Now try to get the corresponding date of requested interval
//     int32 result_date_index = prev_date_index + interval;
//     // This can be useful for reporting or debugging.
//     if (prev_date_index > 0 && first_date_idx != NULL) {
//         *first_date_idx = prev_date_index;
//     }
//     if (result_date_index > 0 && result_date_idx != NULL) {
//         *result_date_idx = result_date_index;
//     }
//     // Now check if inside boundaries.
//     if (result_date_index >= 0) // If result_date_index is positive (negative interval is smaller than current index)
//     {
//         if (prev_date_index < 0) // Handle Negative OOB Indices (When interval is negative)
//         {
//             *result_date = calendar->dates[0]; // Returns first date of the calendar
//             return RET_ADD_DAYS_NEGATIVE;
//         }
//         if (result_date_index >= calendar->dates_size) // Handle Positive OOB Indices (When interval is positive)
//         {
//             *result_date = PG_INT32_MAX; // Returns infinity+.
//             return RET_ADD_DAYS_POSITIVE;
//         }
//         *result_date = calendar->dates[result_date_index];
//         return RET_SUCCESS;
//     } else {
//         // Handle Negative OOB Indices (When interval is negative)
//         *result_date = calendar->dates[0]; // Returns the first date of the calendar.
//         return RET_SUCCESS;
//     }
// }


static DATE_PAST: i32 = crate::PgDate::new(1970, 01, 01).to_epoch();   //1970-01-01
static DATE_FUTURE: i32 = crate::PgDate::new(2199, 01, 01).to_epoch(); //2199-01-01

pub fn add_calendar_days(
    calendar: &Calendar,
    input_date: i32,
    interval: i32,
) -> i32 {
    if calendar.dates.is_empty() {
        return input_date + interval;
    }

    let prev_date_index = get_closest_index_from_left(input_date, calendar);
    let result_date_index = prev_date_index + interval;
    if prev_date_index < 0  || result_date_index < 0 {
        // Handle Negative OOB indices (When interval is negative)
        return DATE_PAST;
    }

    if result_date_index >= calendar.dates.len() as i32 {
        // Returns infinity+
        return DATE_FUTURE;
    }

    return *calendar.dates.get(result_date_index as usize).unwrap();
}
