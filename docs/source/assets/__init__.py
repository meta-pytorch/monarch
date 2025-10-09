import os

from IPython.display import display, HTML


def show_svg(filename: str) -> None:
    """Display an SVG file centered in the notebook.

    Args:
        filename: Name of the SVG file (e.g., 'message_flow.svg')
    """
    cwd = os.getcwd()
    monarch_path, subpath = cwd.rsplit("/docs", maxsplit=1)
    svg_path = os.path.join(monarch_path, "source", "assets", filename)
    with open(svg_path, "r") as f:
        svg_content = f.read()
    html_content = f'<div style="text-align: center;">{svg_content}</div>'
    display(HTML(html_content))
