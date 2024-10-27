import setuptools
 
with open("README.md", "r") as fh:
    long_description = fh.read()
 
setuptools.setup(
    name="spark-json-table-normalization",
    version="0.0.1",
    author="Peter Zeglen",
    author_email="pbzeglen@gmail.com",
    description="Package to properly normalize jsons into multiple tables for fast data processing",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)