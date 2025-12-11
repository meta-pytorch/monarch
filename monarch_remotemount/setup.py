from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="remotemount",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A short description of your package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/remotemount",
    py_modules=["remotemount"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        # Add your dependencies here, e.g.:
        # "requests>=2.25.0",
    ],
    entry_points={
        "console_scripts": [
            # Uncomment if you want a CLI command:
            # "remotemount=remotemount:main",
        ],
    },
)
