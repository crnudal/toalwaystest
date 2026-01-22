def convert_timestamp_to_nanoseconds(timestamp_str: str) -> int:
    """
    Convert timestamp string to nanoseconds since epoch.
    Format: YYYYMMDD-HH:MM:SS.NNNNNNNNN
    """
    try:
        date_part, time_part = timestamp_str.split('-')
        time_components = time_part.split(':')
        
        year = int(date_part[:4])
        month = int(date_part[4:6])
        day = int(date_part[6:8])
        hour = int(time_components[0])
        minute = int(time_components[1])
        second_and_nano = time_components[2].split('.')
        second = int(second_and_nano[0])
        
        # Keep nanoseconds as string to preserve leading zeros
        nanosecond_str = second_and_nano[1]
        
        # Ensure it's exactly 9 digits (pad with zeros if needed)
        nanosecond_str = nanosecond_str.ljust(9, '0')[:9]
        nanosecond = int(nanosecond_str)
        
        dt = datetime(year, month, day, hour, minute, second)
        epoch = datetime(1970, 1, 1)
        
        # Calculate total nanoseconds since epoch
        seconds_since_epoch = (dt - epoch).total_seconds()
        nanoseconds_since_epoch = int(seconds_since_epoch * 1_000_000_000) + nanosecond
        
        return nanoseconds_since_epoch
    except Exception:
        return -1  # Return invalid value if conversion fails
