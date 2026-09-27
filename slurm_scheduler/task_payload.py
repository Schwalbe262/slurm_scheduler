"""Pure helpers shared by task submission paths."""


def prepend_setup_once(generated: str, supplied: str) -> str:
    """Put project setup first without duplicating an already expanded prefix."""
    generated = generated.strip()
    supplied = supplied.strip()
    if not generated:
        return supplied
    if supplied == generated or supplied.startswith(generated + "\n"):
        return supplied
    return f"{generated}\n{supplied}" if supplied else generated
