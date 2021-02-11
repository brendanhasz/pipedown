from setuptools import find_packages, setup

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="drainpype",
    version="0.0.1",
    author="Brendan Hasz",
    author_email="winsto99@gmail.com",
    description="A data science pipelining framework for Python to help you guide the flow of sh*tty data",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url="https://github.com/brendanhasz/drainpype",
    license="MIT",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
    ],
    zip_safe=False,
    install_requires=[
        "matplotlib >= 3.1.0",
        "numpy >= 1.17.0",
        "pandas >= 1.0.0",
        "cloudpickle >= 1.3",
    ],
    extras_require={
        "dev": [
            "autoflake >= 1.4",
            "black >= 19.10b0",
            "bumpversion >= 0.6.0",
            "flake8 >= 3.8.3",
            "isort >= 5.1.2",
            "pytest >= 6.0.0rc1",
            "pytest-cov >= 2.7.1",
            "sphinx >= 3.1.2",
            "sphinx_rtd_theme >= 0.5.0",
            "setuptools >= 49.1.0",
            "twine >= 3.2.0",
            "wheel >= 0.34.2",
        ],
    },
)