import setuptools # type: ignore
# type: ignore

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="jhprofilequota", # Replace with your own username
    version="0.0.1",
    author="Shawn T. O'Neil",
    author_email="oneilsh@gmail.com",
    description="A time- and token-based quota system for JupyterHub.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/oneilsh/jh-profile-quota",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6'
)
