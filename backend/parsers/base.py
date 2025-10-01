"""Compatibility layer for legacy bank parsers."""

from parsers.base_parser import BaseParser


class BaseBankParser(BaseParser):
    """Backward-compatible alias for legacy parsers."""

    def __init__(self, *args, **kwargs):  # pragma: no cover - minimal shim
        super().__init__(*args, **kwargs)


__all__ = ["BaseBankParser"]
