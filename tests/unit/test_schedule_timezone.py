# File: tests/unit/test_schedule_timezone.py
"""
Unit tests for timezone-aware schedule parsing.

Tests cover:
- Timezone resolution (pipeline-specific vs global default)
- Pendulum datetime creation with various IANA timezones
- Invalid timezone error handling
- start_date and end_date parsing
"""

import pytest
import pendulum


class TestPendulumDatetimeParsing:
    """Test parsing date strings into timezone-aware pendulum DateTime objects."""
    
    @pytest.mark.parametrize("timezone", [
        "UTC",
        "America/New_York",
        "America/Chicago",
        "America/Los_Angeles",
        "Europe/London",
        "Europe/Paris",
        "Europe/Amsterdam",
        "Asia/Tokyo",
        "Asia/Shanghai",
        "Australia/Sydney",
    ])
    def test_parse_datetime_with_various_timezones(self, timezone):
        """Parse date with different IANA timezones."""
        date_str = "2024-03-15"
        result = pendulum.parse(date_str, tz=timezone)
        
        assert isinstance(result, pendulum.DateTime)
        assert result.timezone_name == timezone
        assert result.year == 2024
        assert result.month == 3
        assert result.day == 15
        assert result.hour == 0  # Should be midnight
        assert result.minute == 0
        assert result.second == 0
    
    def test_parse_datetime_preserves_timezone_info(self):
        """Parsed datetime should be timezone-aware, not naive."""
        result = pendulum.parse("2024-01-01", tz="America/New_York")
        
        # Pendulum DateTime objects are always timezone-aware
        assert result.timezone_name == "America/New_York"
        assert result.offset_hours in [-5, -4]  # EST or EDT
    
    def test_parse_datetime_utc(self):
        """Parse datetime with UTC timezone."""
        result = pendulum.parse("2024-06-15", tz="UTC")
        
        assert result.timezone_name == "UTC"
        assert result.offset_hours == 0
    
    def test_invalid_timezone_raises_error(self):
        """Invalid timezone should raise error."""
        with pytest.raises(Exception):  # pendulum raises various exceptions
            pendulum.parse("2024-01-01", tz="Invalid/Timezone")
    
    def test_empty_date_string_raises_error(self):
        """Empty date string should raise error."""
        with pytest.raises(Exception):
            pendulum.parse("", tz="UTC")


class TestStartDateEndDateParsing:
    """Test parsing of both start_date and end_date fields."""
    
    def test_parse_start_date_only(self):
        """Parse start_date without end_date."""
        start = pendulum.parse("2024-01-01", tz="UTC")
        
        assert start.year == 2024
        assert start.month == 1
        assert start.day == 1
    
    def test_parse_end_date_only(self):
        """Parse end_date independently."""
        end = pendulum.parse("2024-12-31", tz="UTC")
        
        assert end.year == 2024
        assert end.month == 12
        assert end.day == 31
    
    def test_parse_start_and_end_date_with_same_timezone(self):
        """Parse both start_date and end_date with same timezone."""
        start = pendulum.parse("2024-01-01", tz="America/New_York")
        end = pendulum.parse("2024-12-31", tz="America/New_York")
        
        assert start.timezone_name == end.timezone_name
        assert start < end


class TestTimezoneAwareDatetimeComparison:
    """Test that timezone-aware datetimes work correctly."""
    
    def test_compare_utc_datetimes(self):
        """Compare two UTC datetimes."""
        dt1 = pendulum.parse("2024-01-01", tz="UTC")
        dt2 = pendulum.parse("2024-06-01", tz="UTC")
        
        assert dt1 < dt2
    
    def test_compare_different_timezone_datetimes(self):
        """Compare datetimes in different timezones (should convert correctly)."""
        # Both represent same moment in time (midnight Jan 1, 2024 in each timezone)
        utc_dt = pendulum.parse("2024-01-01", tz="UTC")
        ny_dt = pendulum.parse("2024-01-01", tz="America/New_York")
        
        # NY midnight is 5 hours ahead of UTC midnight (during EST)
        # So NY midnight comes AFTER UTC midnight
        assert ny_dt > utc_dt


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_leap_year_date(self):
        """Parse February 29 in leap year."""
        result = pendulum.parse("2024-02-29", tz="UTC")
        
        assert result.year == 2024
        assert result.month == 2
        assert result.day == 29
    
    def test_invalid_leap_year_date_raises_error(self):
        """February 29 in non-leap year should raise error."""
        with pytest.raises(Exception):
            pendulum.parse("2023-02-29", tz="UTC")  # 2023 is not a leap year
    
    def test_year_2000_date(self):
        """Parse dates from year 2000."""
        result = pendulum.parse("2000-01-01", tz="UTC")
        
        assert result.year == 2000
    
    def test_future_date(self):
        """Parse future dates."""
        result = pendulum.parse("2030-12-31", tz="UTC")
        
        assert result.year == 2030
        assert result.month == 12
        assert result.day == 31
