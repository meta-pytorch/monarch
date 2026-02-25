/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <Python.h>
#include <torch/version.h> // @manual=//caffe2:version_cpp
// @lint-ignore CLANGTIDY facebook-hte-RelativeInclude
#include "mock_cuda.h"

// @lint-ignore-every CLANGTIDY facebook-hte-NullableReturn
static PyObject* getBuiltPytorchVersion(PyObject*, PyObject*) {
  PyObject* tuple = PyTuple_New(3);
  if (!tuple) {
    return nullptr;
  }
  PyObject* major = PyLong_FromLong(TORCH_VERSION_MAJOR);
  if (!major) {
    Py_DECREF(tuple);
    return nullptr;
  }
  PyObject* minor = PyLong_FromLong(TORCH_VERSION_MINOR);
  if (!minor) {
    Py_DECREF(tuple);
    Py_DECREF(major);
    return nullptr;
  }
  PyObject* patch = PyLong_FromLong(TORCH_VERSION_PATCH);
  if (!patch) {
    Py_DECREF(tuple);
    Py_DECREF(major);
    Py_DECREF(minor);
    return nullptr;
  }

  PyTuple_SET_ITEM(tuple, 0, major);
  PyTuple_SET_ITEM(tuple, 1, minor);
  PyTuple_SET_ITEM(tuple, 2, patch);

  return tuple;
}

static PyMethodDef _C_methods[] = {
    {"patch_cuda",
     patch_cuda,
     METH_NOARGS,
     "Initialize the monarch cuda patch."},
    {"mock_cuda", mock_cuda, METH_NOARGS, "Enable cuda mocking."},
    {"unmock_cuda", unmock_cuda, METH_NOARGS, "Disable cuda mocking."},
    {"get_built_pytorch_version",
     getBuiltPytorchVersion,
     METH_NOARGS,
     "Get the version of pytorch monarch was built against."},
    {NULL, NULL, 0, NULL}};

static struct PyModuleDef _C_module = {
    PyModuleDef_HEAD_INIT,
    "_C",
    "A module containing monarch C++ functionality.",
    -1,
    _C_methods,
    NULL,
    NULL,
    NULL,
    NULL};

PyMODINIT_FUNC PyInit__C(void) {
  return PyModule_Create(&_C_module);
}
