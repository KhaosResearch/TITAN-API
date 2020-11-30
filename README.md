## [TITAN platform](https://github.com/benhid/titan-platform) 

---

[![Python](https://img.shields.io/badge/python-3.7-blue.svg?style=flat-square)](https://python.org)
[![License](https://img.shields.io/github/license/benhid/titan-platform.svg?style=flat-square)](https://www.apache.org/licenses/LICENSE-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square)](https://github.com/psf/black)

TITAN is a semantic-enhanced workflow management system that provides several architectural levels to deal with the processes from the design to the execution of workflows:

This repository contains core functionality of the platform.

Before running `titan`, save a copy of [`.env.template`](.env.template) as `.env` and insert your own values. 
`titan` will then look for a valid `.env` file in the **current working directory**. In its absence, it will use environmental variables.

### 🚀 Setup 

#### Installation

Via source code using [Poetry](https://github.com/python-poetry/poetry):

```console
$ git clone https://github.com/benhid/titan-platform.git
$ cd titan-platform
$ poetry install
```

#### Deploy server 

Server can be [deployed](https://fastapi.tiangolo.com/deployment/) with *uvicorn*, a lightning-fast ASGI server, using the command-line client.

```console
$ poetry run titan server
```

Online documentation is available at `/api/docs`.

<p align="center">&mdash; ⭐️ &mdash;</p>
<p align="center"><i>Designed & built by Khaos Research (University of Málaga).</i></p>
