use crate::Calendar;

pub fn calculate_page_size(first_date: i32, last_date: i32, entry_count: i64) -> i32 {
    let date_range = last_date - first_date;
    let avg_entries_per_week_calendar = date_range as f64 / 7.0;
    let entry_count_d = entry_count as f64;

    let mut page_size_tmp = 32; // monthly calendar

    if entry_count_d > avg_entries_per_week_calendar {
        page_size_tmp = 16; // weekly calendar
    }

    page_size_tmp
}


fn left_binary_search(arr: &[i32], mut left: i32, mut right: i32, value: i32) -> i32 {
    while left <= right {
        let mid = left + (right - left) / 2;
        if arr[mid as usize] < value {
            left = mid + 1;
        } else if arr[mid as usize] > value {
            right = mid - 1;
        } else {
            return mid;
        }
    }
    left - 1
}

pub fn get_closest_index_from_left(date: i32, calendar: &Calendar) -> i32 {
    let page_map_index = (date / calendar.page_size) - calendar.first_page_offset;

    if page_map_index >= calendar.page_map.len() as i32 {
        return -1 * calendar.dates.len() as i32 - 1;
    } else if page_map_index < 0 {
        return -1;
    }

    let inclusive_start_index = calendar.page_map[page_map_index as usize];
    let exclusive_end_index = if page_map_index < calendar.page_map.len() as i32 - 1 {
        calendar.page_map[page_map_index as usize + 1]
    } else {
        calendar.dates.len()
    };

    left_binary_search(&calendar.dates, inclusive_start_index as i32, (exclusive_end_index - 1) as i32, date)
}