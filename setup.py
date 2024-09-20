#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-maxcompute"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "1.7.0"
description = """The MaxCompute adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="aliyun",
    author_email="zhangdingxin.zdx@alibaba-inc.com",
    url="https://github.com/dingxin-tech/dbt-maxcompute",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core~=1.7.0.",
        "dbt-common<1.0"
        "dbt-adapter~=0.1.0a2"
    ],
)
