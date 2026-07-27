from typing import Optional


class ConfigLoadError(Exception):
    """Custom exception for configuration loading errors."""

    def __init__(
        self,
        message: str,
        config_file: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        self.config_file = config_file
        self.original_error = original_error
        file_context = f" in {config_file}" if config_file else ""
        super().__init__(f"Configuration error{file_context}: {message}")
