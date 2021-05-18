from dataclasses import dataclass, field

from pendulum.datetime import DateTime


@dataclass(order=True)
class Package:
    # ['filename', 'size_bytes', 'generated_at', 'last_downloaded', 'no_downloads']
    filename: str = field(compare=False)
    size_bytes: int = field(compare=False)
    no_downloads: int = field(default=0, compare=False)
    generated_at: DateTime = field(default=None, compare=False)
    last_downloaded: DateTime = field(default=None, compare=False)
    rank: int = field(default=0, compare=True)
    expired: bool = field(default=False, compare=False)
