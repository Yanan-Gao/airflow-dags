from __future__ import annotations


class SemverVersion:

    def __init__(self, major: int, minor: int, patch: int):
        self.major = major
        self.minor = minor
        self.patch = patch

    @staticmethod
    def from_string(version: str) -> SemverVersion:
        version_parts = version.split(".")
        if len(version_parts) != 3:
            raise ValueError("Invalid Semver version string")
        return SemverVersion(int(version_parts[0]), int(version_parts[1]), int(version_parts[2]))

    def __str__(self):
        return f"{self.major}.{self.minor}.{self.patch}"

    def __repr__(self):
        return f"SemverVersion(major={self.major}, minor={self.minor}, patch={self.patch})"

    def is_compatible_with(self, other: SemverVersion) -> bool:
        return self.major == other.major and self.minor == other.minor

    def __ge__(self, other: SemverVersion) -> bool:
        if isinstance(other, SemverVersion):
            if self.major > other.major:
                return True
            elif self.major == other.major:
                if self.minor > other.minor:
                    return True
                elif self.minor == other.minor:
                    if self.patch >= other.patch:
                        return True
            return False
        raise TypeError("Not supported")

    def __gt__(self, other) -> bool:
        if isinstance(other, SemverVersion):
            if self.major > other.major:
                return True
            elif self.major == other.major:
                if self.minor > other.minor:
                    return True
                elif self.minor == other.minor:
                    if self.patch > other.patch:
                        return True
            return False
        raise TypeError("Not supported")

    def __le__(self, other) -> bool:
        if isinstance(other, SemverVersion):
            if self.major < other.major:
                return True
            elif self.major == other.major:
                if self.minor < other.minor:
                    return True
                elif self.minor == other.minor:
                    if self.patch <= other.patch:
                        return True
            return False
        raise TypeError("Not supported")

    def __lt__(self, other) -> bool:
        if isinstance(other, SemverVersion):
            if self.major < other.major:
                return True
            elif self.major == other.major:
                if self.minor < other.minor:
                    return True
                elif self.minor == other.minor:
                    if self.patch < other.patch:
                        return True
            return False
        raise TypeError("Not supported")

    def __eq__(self, other):
        if isinstance(other, SemverVersion):
            return self.major == other.major and self.minor == other.minor and self.patch == other.patch
        raise TypeError("Not supported")
