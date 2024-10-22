<p align="left">
  <img src="icon_MaxCompute.svg" alt="MaxCompute logo" width="300" height="150" style="margin-right: 100px;"/>
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="300" height="150"/>
</p>

# dbt-maxcompute

Welcome to the **dbt-maxCompute** repository! This project aims to extend the capabilities of **dbt** (data build tool)
for users of Alibaba MaxCompute, a cutting-edge data processing platform.

## What is dbt?

**[dbt](https://www.getdbt.com/)** empowers data analysts and engineers to transform their data using software
engineering best practices. It serves as the **T** in the ELT (Extract, Load, Transform) process, allowing users to
organize, cleanse, denormalize, filter, rename, and pre-aggregate raw data, making it analysis-ready.

## About MaxCompute

MaxCompute is Alibaba Group's cloud data warehouse and big data processing platform, supporting massive data storage and
computation, widely used for data analysis and business intelligence. With MaxCompute, users can efficiently manage and
analyze large volumes of data and gain real-time business insights.

This repository contains the foundational code for the **dbt-maxcompute** adapter plugin. For guidance on developing the
adapter, please refer to the [official documentation](https://docs.getdbt.com/docs/contributing/building-a-new-adapter).

### Important Note

The `README` you are currently viewing will be updated with specific instructions and details on how to utilize the
adapter as development progresses.

### Adapter Versioning

This adapter plugin follows [semantic versioning](https://semver.org/). The initial version is **v1.8.0-a1**, designed
for compatibility with dbt Core v1.8.0. Since the plugin is in its early stages, the version number **a1** indicates
that it is an Alpha 1 release. A stable version will be released in the future, focusing on MaxCompute-specific
functionality and aiming for backwards compatibility.

## Getting Started

### For Users

To install the plugin, run the following command:

```bash
pip install dbt-maxcompute==1.8.0a1
```

You will also need to configure your dbt profile with the following settings:

```json
{
  "type": "maxcompute",
  "project": "<your_project>",
  "endpoint": "<your_endpoint>",
  "accessId": "<your_access_id>",
  "accessKey": "<your_access_key>",
  "schema": "<your_namespace_schema>"
}
```

### For Developers

If you want to contribute or develop the adapter, use the following command to set up your environment:

```bash
pip install -r dev-requirements.txt
```

## Reporting Bugs and Contributing

Your feedback helps improve the project:

- To report bugs or request features, please open a
  new [issue](https://github.com/aliyun/dbt-maxcompute/issues/new) on GitHub.

## Code of Conduct

We are committed to fostering a welcoming and inclusive environment. All community members are expected to adhere to
the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).