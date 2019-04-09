import setuptools

setuptools.setup(
    name = "web-scraping",
    version = "0.1.0",
    zip_safe = False,
    packages=[ "financial" ],
    install_requires=[
        "mechanize",
        "bs4",
        "requests",
    ],
)

