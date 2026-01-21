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
        nanosecond = int(second_and_nano[1])
        
        dt = datetime(year, month, day, hour, minute, second)
        epoch = datetime(1970, 1, 1)
        
        # Calculate total nanoseconds since epoch
        seconds_since_epoch = (dt - epoch).total_seconds()
        nanoseconds_since_epoch = int(seconds_since_epoch * 1_000_000_000) + nanosecond
        
        return nanoseconds_since_epoch
    except Exception:
        return -1  # Return invalid value if conversion fails


# Define timestamp checks once for reuse
timestamp_checks = [
    Check(
        lambda s: s.str.match(r'^\d{8}-\d{2}:\d{2}:\d{2}\.\d{9}$').all(),
        error="Timestamp must be in format YYYYMMDD-HH:MM:SS.NNNNNNNNN"
    ),
    Check(
        lambda s: s.str[:8].apply(
            lambda x: pd.to_datetime(x, format='%Y%m%d', errors='coerce')
        ).notna().all(),
        error="Date part must be valid"
    ),
    Check(
        lambda s: s.str.split('-').str[1].str.split(':').str[0].astype(int).between(0, 23).all(),
        error="Hours must be between 0-23"
    ),
    Check(
        lambda s: s.str.split('-').str[1].str.split(':').str[1].astype(int).between(0, 59).all(),
        error="Minutes must be between 0-59"
    ),
    Check(
        lambda s: s.str.split('-').str[1].str.split(':').str[2].str.split('.').str[0].astype(int).between(0, 59).all(),
        error="Seconds must be between 0-59"
    ),
    Check(
        lambda s: s.str.split('.').str[1].str.len().eq(9).all(),
        error="Nanoseconds must have exactly 9 digits"
    ),
]


# Pandera Schema for timestamp validation
timestamp_schema = DataFrameSchema(
    {
        "timestamp": Column(str, checks=timestamp_checks, nullable=False),
        "timestamp_1": Column(str, checks=timestamp_checks, nullable=False),
        "timesequence": Column(
            pa.Int64,
            checks=[
                Check(lambda s: s.notna().all(), error="Timesequence cannot be null"),
                Check(lambda s: (s > 0).all(), error="Timesequence must be positive"),
                Check(
                    lambda s: s.astype(str).str.len().eq(19).all(),
                    error="Timesequence must have exactly 19 digits"
                ),
                Check(
                    lambda s: ~s.astype(str).str[-3:].eq('000').any(),
                    error="Timesequence last 3 digits cannot be 000"
                ),
            ],
            nullable=False
        )
    },
    checks=[
        # DataFrame-level check: timesequence must equal converted timestamp
        Check(
            lambda df: (
                df['timesequence'] == df['timestamp'].apply(convert_timestamp_to_nanoseconds)
            ).all(),
            error="Timesequence must equal timestamp converted to nanoseconds"
        )
    ],
    strict=False,  # Allow other columns
    coerce=True
)
