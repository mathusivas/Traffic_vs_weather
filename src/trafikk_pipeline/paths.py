from datetime import date

def part_dir(d: date, base: str) -> str:
    return f"{base}/{d.year}/{d.month:02d}/{d.day:02d}"
