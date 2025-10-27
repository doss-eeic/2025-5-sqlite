#ifndef PYUDF_H
#define PYUDF_H

#define PY_SSIZE_T_CLEAN
#include "sqlite3.h"
#include <Python.h>


int		register_pyfunc(sqlite3 *db, const char *zName, int nArg, PyObject *callable);

#endif
