from setuptools import setup, Extension

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

fast_pack_ext = Extension(
    "remotemount._fast_pack",
    sources=["remotemount/_fast_pack.c"],
    extra_compile_args=["-O3", "-pthread"],
    extra_link_args=["-pthread"],
)

setup(
    name="remotemount",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A short description of your package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/remotemount",
    packages=["remotemount"],
    ext_modules=[fast_pack_ext],
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
