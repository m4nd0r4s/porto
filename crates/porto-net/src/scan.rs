//! Searching a stream's buffered bytes for a separator, without copying them
//! (17-networking-tls-dns.md §1.1: `read_until`, `readline`).

/// The index where `needle` first occurs in `haystack` at or after `from`, or `None`.
///
/// A scan that found nothing in a buffer which then grew can resume at
/// `searched.saturating_sub(needle.len() - 1)`: a match cannot start earlier without having
/// been found, and one that straddles the old end starts at most `needle.len() - 1` bytes
/// before it, so a delimiter split over many small segments is found without scanning the
/// buffer again from the start.
///
/// The cost is `O(haystack.len() * needle.len())` in the worst case (a needle whose first byte
/// is everywhere), which is nothing for the short delimiters protocols use.
///
/// # Panics
///
/// If `needle` is empty.
#[must_use]
pub fn find(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    let (&first, rest) = needle.split_first().expect("the separator is not empty");
    let mut start = from;
    while let Some(window) = haystack.get(start..) {
        if window.len() < needle.len() {
            return None;
        }
        // The last start with room for the whole needle bounds the search for its first byte.
        let last_start = window.len() - needle.len();
        let candidate = window[..=last_start]
            .iter()
            .position(|&byte| byte == first)?;
        let at = start + candidate;
        if &haystack[at + 1..at + needle.len()] == rest {
            return Some(at);
        }
        start = at + 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_the_first_occurrence() {
        assert_eq!(find(b"abc\ndef\n", b"\n", 0), Some(3));
        assert_eq!(find(b"abc\ndef\n", b"\n", 4), Some(7));
        assert_eq!(find(b"abc\r\ndef", b"\r\n", 0), Some(3));
        assert_eq!(find(b"\n", b"\n", 0), Some(0));
    }

    #[test]
    fn a_missing_needle_is_none() {
        assert_eq!(find(b"", b"\n", 0), None);
        assert_eq!(find(b"abc", b"\n", 0), None);
        assert_eq!(find(b"abc\r", b"\r\n", 0), None);
        assert_eq!(find(b"abc", b"abcd", 0), None);
        assert_eq!(find(b"abc", b"c", 3), None);
        assert_eq!(find(b"abc", b"c", 99), None);
    }

    #[test]
    fn a_false_start_does_not_hide_a_match() {
        assert_eq!(find(b"aaab", b"aab", 0), Some(1));
        assert_eq!(find(b"\r\r\n", b"\r\n", 0), Some(1));
    }

    #[test]
    fn a_match_split_over_segments_is_found_by_resuming_before_the_old_end() {
        let separator = b"\r\n\r\n";
        let mut buffer = Vec::new();
        let mut searched: usize = 0;
        let mut found = None;
        for segment in [b"GET /".as_slice(), b" HTTP/1.1\r", b"\n", b"\r", b"\nbody"] {
            buffer.extend_from_slice(segment);
            let from = searched.saturating_sub(separator.len() - 1);
            found = find(&buffer, separator, from);
            if found.is_some() {
                break;
            }
            searched = buffer.len();
        }
        assert_eq!(found, Some(buffer.len() - b"body".len() - separator.len()));
        assert_eq!(found, find(&buffer, separator, 0));
    }

    #[test]
    #[should_panic(expected = "not empty")]
    fn an_empty_needle_is_a_bug_of_the_caller() {
        let _ = find(b"abc", b"", 0);
    }
}
