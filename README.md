# midrc-interop
The MIDRC Interoperability Tool is a suite of Gen3 mesh software services and API adaptors written in Python that enable medical imaging data repositories to participate in a data mesh by sharing data, communicating, and exchanging information with other data resource, commons, and hubs. 

## Prerequisites
This project is built with Python. Ensure you have Python 3.9 or later installed.

## Gen3 SDK for Python

The MIDRC interoperability tools for discovery metadata aggregation use the Gen3 SDK extensively, which can be found in GitHub at https://github.com/uc-cdis/gen3sdk-python. The Gen3 Software Development Kit (SDK) for Python provides classes and functions for handling common tasks when interacting with a Gen3 commons. It also exposes a Command Line Interface (CLI). Working with APIs can be overwhelming, so the MIDRC API adaptors use the Gen3 Python SDK/CLI to simplify communication with various nodes in the biomedical imaging data mesh.

## REST-API
The tools utilized for MIDRC interact with the Gen3 API. For more information, see the [service specifications](https://docs.gen3.org/gen3-resources/user-guide/using-api/) of the Gen3 API.

## Contributing
Check out our [CONTRIBUTING.md](CONTRIBUTING.md) doc to learn how to contribute.
