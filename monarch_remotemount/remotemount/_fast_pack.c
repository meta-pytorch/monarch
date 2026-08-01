/*
 * Fast file packing using mmap + parallel reads.
 * Accepts file list from Python to ensure consistent ordering with metadata.
 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#define NUM_THREADS 16
#define MAP_POPULATE 0x008000

typedef struct {
    const char *path;
    size_t offset;
    size_t size;
} FileInfo;

typedef struct {
    FileInfo *files;
    int start_idx;
    int end_idx;
    char *buffer;
} ThreadArg;

static void *read_files_thread(void *arg) {
    ThreadArg *ta = (ThreadArg *)arg;
    char *buffer = ta->buffer;

    for (int i = ta->start_idx; i < ta->end_idx; i++) {
        FileInfo *f = &ta->files[i];
        if (f->size == 0) continue;

        int fd = open(f->path, O_RDONLY);
        if (fd < 0) continue;

        size_t total_read = 0;
        while (total_read < f->size) {
            ssize_t bytes_read = read(fd, buffer + f->offset + total_read, f->size - total_read);
            if (bytes_read <= 0) break;
            total_read += bytes_read;
        }
        close(fd);
    }
    return NULL;
}

/*
 * pack_files_with_offsets(file_list, total_size)
 *
 * Pack files into a buffer given a list of (path, offset, size) tuples.
 * Returns a memoryview of the packed buffer.
 */
static PyObject *pack_files_with_offsets(PyObject *self, PyObject *args) {
    PyObject *file_list;
    unsigned long long total_size;

    if (!PyArg_ParseTuple(args, "OK", &file_list, &total_size)) {
        return NULL;
    }

    if (!PyList_Check(file_list)) {
        PyErr_SetString(PyExc_TypeError, "First argument must be a list");
        return NULL;
    }

    Py_ssize_t num_files = PyList_Size(file_list);
    if (num_files == 0 || total_size == 0) {
        /* Return empty bytes */
        return PyBytes_FromStringAndSize(NULL, 0);
    }

    /* Parse file list into FileInfo array */
    FileInfo *files = (FileInfo *)malloc(num_files * sizeof(FileInfo));
    if (!files) {
        PyErr_NoMemory();
        return NULL;
    }

    for (Py_ssize_t i = 0; i < num_files; i++) {
        PyObject *item = PyList_GET_ITEM(file_list, i);
        PyObject *path_obj, *offset_obj, *size_obj;

        if (!PyTuple_Check(item) || PyTuple_Size(item) != 3) {
            free(files);
            PyErr_SetString(PyExc_TypeError, "Each item must be a (path, offset, size) tuple");
            return NULL;
        }

        path_obj = PyTuple_GET_ITEM(item, 0);
        offset_obj = PyTuple_GET_ITEM(item, 1);
        size_obj = PyTuple_GET_ITEM(item, 2);

        if (!PyUnicode_Check(path_obj)) {
            free(files);
            PyErr_SetString(PyExc_TypeError, "Path must be a string");
            return NULL;
        }

        files[i].path = PyUnicode_AsUTF8(path_obj);
        files[i].offset = PyLong_AsUnsignedLongLong(offset_obj);
        files[i].size = PyLong_AsUnsignedLongLong(size_obj);

        if (PyErr_Occurred()) {
            free(files);
            return NULL;
        }
    }

    /* Allocate mmap buffer with MAP_POPULATE */
    char *buffer = (char *)mmap(NULL, (size_t)total_size,
                                PROT_READ | PROT_WRITE,
                                MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,
                                -1, 0);
    if (buffer == MAP_FAILED) {
        free(files);
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    /* Parallel file reading */
    pthread_t threads[NUM_THREADS];
    ThreadArg thread_args[NUM_THREADS];
    int files_per_thread = (num_files + NUM_THREADS - 1) / NUM_THREADS;

    for (int t = 0; t < NUM_THREADS; t++) {
        thread_args[t].files = files;
        thread_args[t].start_idx = t * files_per_thread;
        thread_args[t].end_idx = (t + 1) * files_per_thread;
        if (thread_args[t].end_idx > num_files) {
            thread_args[t].end_idx = num_files;
        }
        thread_args[t].buffer = buffer;
        pthread_create(&threads[t], NULL, read_files_thread, &thread_args[t]);
    }

    for (int t = 0; t < NUM_THREADS; t++) {
        pthread_join(threads[t], NULL);
    }

    free(files);

    /* Create memoryview from mmap buffer - must be writable for RDMA */
    PyObject *result = PyMemoryView_FromMemory(buffer, (Py_ssize_t)total_size, PyBUF_WRITE);
    return result;
}

static PyMethodDef FastPackMethods[] = {
    {"pack_files_with_offsets", pack_files_with_offsets, METH_VARARGS,
     "Pack files into a buffer given a list of (path, offset, size) tuples."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef fastpackmodule = {
    PyModuleDef_HEAD_INIT,
    "_fast_pack",
    "Fast file packing using mmap + parallel reads",
    -1,
    FastPackMethods
};

PyMODINIT_FUNC PyInit__fast_pack(void) {
    return PyModule_Create(&fastpackmodule);
}
