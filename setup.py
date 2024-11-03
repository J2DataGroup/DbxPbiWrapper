from setuptools import setup, find_packages

setup(
    name="ReservesPackage",
    author="J2DataGroup",
    author_email="info@j2datagroup.com",
    description="Dbx to Pbi Wrapper",
    version="0.1",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)