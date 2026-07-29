# File: tests/unit/test_dst_transitions.py
"""
Unit tests for Daylight Saving Time (DST) transition handling using pendulum.

Tests cover:
- Spring forward (clocks move ahead) transitions
- Fall back (clocks move back) transitions
- Non-existent times (2:30am on spring-forward day)
- Ambiguous times (1:30am on fall-back day)
- Multiple timezone DST transitions (US, Europe, Southern Hemisphere)
"""

from typing import cast

import pytest
import pendulum


def _parse(date_str: str, tz: str) -> pendulum.DateTime:
    """Typed wrapper: pendulum.parse() returns Date|Time|DateTime|Duration per
    its stubs, but a date-only string with an explicit tz always yields a
    DateTime at runtime."""
    return cast(pendulum.DateTime, pendulum.parse(date_str, tz=tz))


class TestSpringForwardTransitions:
    """Test spring forward DST transitions (clocks jump ahead 1 hour)."""
    
    @pytest.mark.parametrize("timezone,spring_forward_date", [
        ("America/New_York", "2024-03-10"),  # Second Sunday in March
        ("America/Chicago", "2024-03-10"),
        ("America/Los_Angeles", "2024-03-10"),
        ("Europe/London", "2024-03-31"),  # Last Sunday in March
        ("Europe/Paris", "2024-03-31"),
        ("Europe/Amsterdam", "2024-03-31"),
    ])
    def test_parse_date_before_spring_forward(self, timezone, spring_forward_date):
        """Parse date before spring forward transition."""
        # Day before spring forward
        dt_before = _parse(spring_forward_date, tz=timezone).subtract(days=1)
        
        # Should be in standard time (not DST)
        assert not dt_before.is_dst()
    
    @pytest.mark.parametrize("timezone,spring_forward_date", [
        ("America/New_York", "2024-03-10"),
        ("America/Chicago", "2024-03-10"),
        ("Europe/London", "2024-03-31"),
        ("Europe/Paris", "2024-03-31"),
    ])
    def test_parse_date_after_spring_forward(self, timezone, spring_forward_date):
        """Parse date after spring forward transition."""
        # Day after spring forward
        dt_after = _parse(spring_forward_date, tz=timezone).add(days=1)
        
        # Should be in daylight saving time
        assert dt_after.is_dst()
    
    def test_non_existent_time_during_spring_forward(self):
        """
        Test handling of non-existent time during spring forward.
        
        On March 10, 2024 in America/New_York:
        - At 2:00 AM, clocks jump to 3:00 AM
        - Times between 2:00 AM and 3:00 AM don't exist
        
        Pendulum should handle this by moving to next valid time.
        """
        timezone = "America/New_York"
        
        # Try to create 2:30 AM on spring-forward day (non-existent)
        # Pendulum will adjust this to a valid time
        dt = pendulum.datetime(2024, 3, 10, 2, 30, tz=timezone)
        
        # Pendulum typically moves forward to 3:30 AM
        assert dt.hour == 3
        assert dt.minute == 30
        assert dt.is_dst()


class TestFallBackTransitions:
    """Test fall back DST transitions (clocks jump back 1 hour)."""
    
    @pytest.mark.parametrize("timezone,fall_back_date", [
        ("America/New_York", "2024-11-03"),  # First Sunday in November
        ("America/Chicago", "2024-11-03"),
        ("America/Los_Angeles", "2024-11-03"),
        ("Europe/London", "2024-10-27"),  # Last Sunday in October
        ("Europe/Paris", "2024-10-27"),
        ("Europe/Amsterdam", "2024-10-27"),
    ])
    def test_parse_date_before_fall_back(self, timezone, fall_back_date):
        """Parse date before fall back transition."""
        # Day before fall back
        dt_before = _parse(fall_back_date, tz=timezone).subtract(days=1)
        
        # Should still be in DST
        assert dt_before.is_dst()
    
    @pytest.mark.parametrize("timezone,fall_back_date", [
        ("America/New_York", "2024-11-03"),
        ("America/Chicago", "2024-11-03"),
        ("Europe/London", "2024-10-27"),
        ("Europe/Paris", "2024-10-27"),
    ])
    def test_parse_date_after_fall_back(self, timezone, fall_back_date):
        """Parse date after fall back transition."""
        # Day after fall back
        dt_after = _parse(fall_back_date, tz=timezone).add(days=1)
        
        # Should be in standard time (not DST)
        assert not dt_after.is_dst()
    
    def test_ambiguous_time_during_fall_back(self):
        """
        Test handling of ambiguous time during fall back.
        
        On November 3, 2024 in America/New_York:
        - At 2:00 AM, clocks jump back to 1:00 AM
        - Times between 1:00 AM and 2:00 AM occur twice
        
        Pendulum handles ambiguous times by choosing the fold parameter.
        """
        timezone = "America/New_York"
        
        # 1:30 AM exists twice on fall-back day
        # First occurrence (DST, fold=0)
        dt_first = pendulum.datetime(2024, 11, 3, 1, 30, tz=timezone, fold=0)
        assert dt_first.is_dst()
        
        # Second occurrence (Standard, fold=1)
        dt_second = pendulum.datetime(2024, 11, 3, 1, 30, tz=timezone, fold=1)
        assert not dt_second.is_dst()
        
        # Both represent the same wall time but different UTC times
        # Convert to UTC to verify they're different moments
        dt_first_utc = dt_first.in_timezone("UTC")
        dt_second_utc = dt_second.in_timezone("UTC")
        assert dt_first_utc < dt_second_utc


class TestCronVsTimedeltaDSTBehavior:
    """
    Test that cron schedules adjust for DST while timedelta schedules do not.
    
    From Airflow docs:
    - Cron schedules: Respect DST, execution time shifts
    - Timedelta schedules: Fixed duration, execution time constant in UTC
    """
    
    def test_cron_daily_midnight_shifts_with_dst(self):
        """
        Daily cron at midnight should shift in UTC during DST transitions.
        
        Example: @daily in America/New_York
        - Winter (EST): midnight = 5:00 UTC
        - Summer (EDT): midnight = 4:00 UTC
        """
        timezone = "America/New_York"
        
        # Winter: January (EST, UTC-5)
        winter_midnight = pendulum.datetime(2024, 1, 15, 0, 0, tz=timezone)
        winter_utc = winter_midnight.in_timezone("UTC")
        assert winter_utc.hour == 5  # Midnight EST = 5am UTC
        
        # Summer: July (EDT, UTC-4)
        summer_midnight = pendulum.datetime(2024, 7, 15, 0, 0, tz=timezone)
        summer_utc = summer_midnight.in_timezone("UTC")
        assert summer_utc.hour == 4  # Midnight EDT = 4am UTC
    
    def test_timedelta_schedule_constant_in_utc(self):
        """
        Timedelta schedules maintain constant UTC interval regardless of DST.
        
        Example: timedelta(days=1) always means 24 hours apart in UTC.
        """
        timezone = "America/New_York"
        
        # Start in winter
        start = pendulum.datetime(2024, 1, 15, 0, 0, tz=timezone)
        
        # Add 24 hours (timedelta behavior)
        next_run = start.add(days=1)
        
        # Should be exactly 24 hours later in UTC
        diff = next_run.diff(start)
        assert diff.in_hours() == 24


class TestMultipleTimezoneDST:
    """Test DST transitions across different geographic regions."""
    
    def test_us_dst_transitions_2024(self):
        """Test US DST transitions for 2024."""
        # US: Second Sunday in March, First Sunday in November
        
        tz = "America/New_York"
        
        # Before spring forward (March 9)
        before = pendulum.datetime(2024, 3, 9, 12, 0, tz=tz)
        assert not before.is_dst()
        
        # After spring forward (March 11)
        after = pendulum.datetime(2024, 3, 11, 12, 0, tz=tz)
        assert after.is_dst()
        
        # Before fall back (November 2)
        before_fall = pendulum.datetime(2024, 11, 2, 12, 0, tz=tz)
        assert before_fall.is_dst()
        
        # After fall back (November 4)
        after_fall = pendulum.datetime(2024, 11, 4, 12, 0, tz=tz)
        assert not after_fall.is_dst()
    
    def test_europe_dst_transitions_2024(self):
        """Test European DST transitions for 2024."""
        # Europe: Last Sunday in March, Last Sunday in October
        
        tz = "Europe/London"
        
        # Before spring forward (March 30)
        before = pendulum.datetime(2024, 3, 30, 12, 0, tz=tz)
        assert not before.is_dst()
        
        # After spring forward (April 1)
        after = pendulum.datetime(2024, 4, 1, 12, 0, tz=tz)
        assert after.is_dst()
        
        # Before fall back (October 26)
        before_fall = pendulum.datetime(2024, 10, 26, 12, 0, tz=tz)
        assert before_fall.is_dst()
        
        # After fall back (October 28)
        after_fall = pendulum.datetime(2024, 10, 28, 12, 0, tz=tz)
        assert not after_fall.is_dst()
    
    def test_southern_hemisphere_dst_opposite_cycle(self):
        """
        Test Southern Hemisphere DST (opposite of Northern Hemisphere).
        
        Australia: DST from October to April (opposite of US/Europe)
        """
        tz = "Australia/Sydney"
        
        # January (summer in Australia): DST active
        summer = pendulum.datetime(2024, 1, 15, 12, 0, tz=tz)
        assert summer.is_dst()
        
        # July (winter in Australia): DST inactive
        winter = pendulum.datetime(2024, 7, 15, 12, 0, tz=tz)
        assert not winter.is_dst()


class TestDSTEdgeCases:
    """Test edge cases and boundary conditions for DST."""
    
    def test_timezone_without_dst(self):
        """Test timezones that don't observe DST."""
        # Arizona (most of it) doesn't observe DST
        tz = "America/Phoenix"
        
        # Summer
        summer = pendulum.datetime(2024, 7, 15, 12, 0, tz=tz)
        assert not summer.is_dst()
        
        # Winter
        winter = pendulum.datetime(2024, 1, 15, 12, 0, tz=tz)
        assert not winter.is_dst()
    
    def test_utc_never_observes_dst(self):
        """UTC never observes DST."""
        # Summer
        summer = pendulum.datetime(2024, 7, 15, 12, 0, tz="UTC")
        assert not summer.is_dst()
        
        # Winter
        winter = pendulum.datetime(2024, 1, 15, 12, 0, tz="UTC")
        assert not winter.is_dst()
    
    def test_hawaii_no_dst(self):
        """Hawaii doesn't observe DST."""
        tz = "Pacific/Honolulu"
        
        summer = pendulum.datetime(2024, 7, 15, 12, 0, tz=tz)
        assert not summer.is_dst()
        
        winter = pendulum.datetime(2024, 1, 15, 12, 0, tz=tz)
        assert not winter.is_dst()


class TestDSTTransitionDates2024To2026:
    """Test specific DST transition dates for 2024-2026."""
    
    @pytest.mark.parametrize("year,spring_date,fall_date", [
        (2024, "2024-03-10", "2024-11-03"),
        (2025, "2025-03-09", "2025-11-02"),
        (2026, "2026-03-08", "2026-11-01"),
    ])
    def test_us_dst_transitions_multi_year(self, year, spring_date, fall_date):
        """Test US DST transitions for multiple years."""
        tz = "America/New_York"
        
        # Day before spring forward: EST
        before_spring = _parse(spring_date, tz=tz).subtract(days=1).at(12, 0)
        assert not before_spring.is_dst()
        
        # Day after spring forward: EDT
        after_spring = _parse(spring_date, tz=tz).add(days=1).at(12, 0)
        assert after_spring.is_dst()
        
        # Day before fall back: EDT
        before_fall = _parse(fall_date, tz=tz).subtract(days=1).at(12, 0)
        assert before_fall.is_dst()
        
        # Day after fall back: EST
        after_fall = _parse(fall_date, tz=tz).add(days=1).at(12, 0)
        assert not after_fall.is_dst()

