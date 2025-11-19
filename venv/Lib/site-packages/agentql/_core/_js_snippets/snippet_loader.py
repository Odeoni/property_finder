import os


def load_js(filename: str) -> str:
    """
    Load a JavaScript file and return its content as a string.
    """
    if not filename.endswith(".js"):
        filename += ".js"

    filename = os.path.join(os.path.dirname(__file__), filename)

    if not os.path.exists(filename):
        raise FileNotFoundError(f"JS Snippet '{filename}' not found")

    with open(filename, encoding="utf-8") as f:
        return f.read()
