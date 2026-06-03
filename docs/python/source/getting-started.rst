###############
Getting Started
###############

************
Installation
************

qvdrs is a Python library available through `pypi <https://pypi.org/>`_. The recommended way to
install and maintain qvdrs as a dependency is through the package installer (PIP). Before
installing this library, download and install Python.

To use qvdrs, first install it using pip:

.. code-block:: console

   pip install qvdrs

Optional integrations with pandas, polars and DuckDB:

.. code-block:: bash

   pip install qvdrs[pandas]
   pip install qvdrs[polars]
   pip install qvdrs[duckdb]
   pip install qvdrs[all]

**********
Quickstart
**********

Read a QVD file directly into a pandas DataFrame:

.. code-block:: python

   import qvd

   table = qvd.read_qvd("data.qvd")
   table.save("copy.qvd")
