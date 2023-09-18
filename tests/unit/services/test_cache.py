import os
import time
import pendulum

from download.dto import Package
from download.services.cache import select_packages_to_be_removed

os.environ["TZ"] = "UTC"
time.tzset()


def test_select_packages():
    today = pendulum.now()
    gb = 1073741824
    packages = [
        Package(
            "ranked1",
            600,
            10,
            generated_at=today.subtract(days=8),
            last_downloaded=today.subtract(days=3),
        ),
        Package(
            "ranked2",
            600,
            20,
            generated_at=today.subtract(days=8),
            last_downloaded=today.subtract(days=3),
        ),
        Package(
            "ranked3",
            600,
            5,
            generated_at=today.subtract(days=8),
            last_downloaded=today.subtract(days=3),
        ),
        Package(
            "ranked4",
            40,
            5,
            generated_at=today.subtract(days=8),
            last_downloaded=today.subtract(days=3),
        ),
        Package(
            "ranked5",
            40,
            5,
            generated_at=today.subtract(days=8),
            last_downloaded=today.subtract(days=2),
        ),
        Package(
            "lt than gb",
            gb - 1,
            1,
            generated_at=today.subtract(days=1),
            last_downloaded=today.subtract(days=1),
        ),
        Package(
            "gt than gb",
            gb + 1,
            1,
            generated_at=today.subtract(days=1),
            last_downloaded=today.subtract(days=1),
        ),
        Package(
            "gt than 10 gb",
            gb * 10,
            1,
            generated_at=today.subtract(days=1),
            last_downloaded=today.subtract(days=1),
        ),
        Package("expired1", 20, 0, generated_at=today.subtract(days=8)),
        Package("expired2", 20, 0, generated_at=today.subtract(days=31)),
    ]
    rem, exp, ranked = select_packages_to_be_removed(300, packages)

    assert ranked[0].filename == "gt than 10 gb"
    assert ranked[7].filename == "ranked2"
    assert ranked[4].rank < ranked[5].rank  # More recent package has higher rank
    assert (
        ranked[2].rank > ranked[1].rank
    )  # less than GB should have higher rank than over 1 GB file
    assert (
        ranked[1].rank > ranked[0].rank
    )  # less than 10 GB should have higher rank than over 10 GB
    assert len(rem) == 3
    assert len(exp) == 2
