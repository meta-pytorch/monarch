/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <Python.h>
#include <torch/version.h> // @manual=//caffe2:version_cpp
#ifdef MONARCH_HAS_CUDA
// @lint-ignore CLANGTIDY facebook-hte-RelativeInclude
#include "mock_cuda.h"
#endif

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
#ifdef MONARCH_HAS_CUDA
    {"patch_cuda",
     patch_cuda,
     METH_NOARGS,
     "Initialize the monarch cuda patch."},
    {"mock_cuda", mock_cuda, METH_NOARGS, "Enable cuda mocking."},
    {"unmock_cuda", unmock_cuda, METH_NOARGS, "Disable cuda mocking."},
#endif
    {"get_built_pytorch_version",
     getBuiltPytorchVersion,
     METH_NOARGS,
     "Get the version of pytorch monarch was built against."},
    {NULL, NULL, 0, NULL}};

static int module_exec(PyObject* /* module */) {
  return 0;
}

static PyModuleDef_Slot _C_slots[] = {
    {Py_mod_exec, (void*)module_exec},
#if PY_VERSION_HEX >= 0x030D0000
    {Py_mod_gil, Py_MOD_GIL_USED},
#endif
    {0, NULL}};

static struct PyModuleDef _C_module = {
    PyModuleDef_HEAD_INIT,
    "_C",
    "A module containing monarch C++ functionality.",
    0,
    _C_methods,
    _C_slots,
    NULL,
    NULL,
    NULL};

PyMODINIT_FUNC PyInit__C(void) {
  return PyModuleDef_Init(&_C_module);
}
