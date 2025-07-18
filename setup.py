from setuptools import setup, find_packages

setup(
    name="visual_layer_sdk",
    version="0.1.7",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests>=2.31.0",
        "pathlib>=1.0.1",
    ],
    author="Visual Layer",
    author_email="support@visuallayer.com",
    description="Python SDK for Visual Layer API",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/visuallayer/python-sdk",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
