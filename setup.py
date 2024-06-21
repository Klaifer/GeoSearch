import setuptools
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setuptools.setup(
    name="GeoSearch",
    version="0.0.1",
    author="Klaifer Garcia",
    description="GeoNames offline query tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Klaifer/geosearch",
    packages=["GeoSearch"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    package_dir={'GeoSearch':'src/GeoSearch'},
    install_requires=["Whoosh>=2.7.4", "beautifulsoup4", "lxml", "tqdm"],
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "GeoSearch = GeoSearch.geosearch:__main__"
        ]
    }
)