def print_header(title: str) -> None:
    """Prints a formatted section header."""

    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def print_success(message: str) -> None:
    """Prints a success message."""

    print(f"✔ {message}")


def print_error(message: str) -> None:
    """Prints an error message."""

    print(f"✘ {message}")


def print_info(message: str) -> None:
    """Prints an informational message."""

    print(f"ℹ {message}")