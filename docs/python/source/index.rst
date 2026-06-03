#################################
Welcome to qvdrs's documentation!
#################################

.. toctree::
   :maxdepth: 3
   :caption: Contents:
   :hidden:

   Home <self>
   getting-started
   api

**qvdrs** is a Rust library that provides a simple API for reading and writing Qlik View Data (QVD) files. Using this
library, it is possible to parse the binary QVD file format and convert it into native Rust data structures or serialize them back.

Built on top of the high-performance Rust core, qvdrs also exposes Python bindings — allowing you to leverage the speed and safety
of Rust while working in a familiar Python environment. Optional features, such as connectors to third-party ecosystems like Apache
Arrow or Polars, are gated behind feature flags on the Rust side and optional dependencies on the Python side.

*******
License
*******

PyQvd is licensed under the `MIT License <https://opensource.org/licenses/MIT>`_.

Copyright (c) 2026 Stanislav Chernov (@bintocher)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
