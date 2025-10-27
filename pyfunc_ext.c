#include "pyudf.h"

static void set_sqlite_result_from_py(sqlite3_context *ctx, PyObject *r) {
  if (r == Py_None) {
    sqlite3_result_null(ctx);
    return;
  }
  if (PyLong_Check(r)) {
    sqlite3_result_int64(ctx, PyLong_AsLongLong(r));
    return;
  }
  if (PyFloat_Check(r)) {
    sqlite3_result_double(ctx, PyFloat_AsDouble(r));
    return;
  }
  if (PyUnicode_Check(r)) {
    Py_ssize_t n = 0;
    const char *s = PyUnicode_AsUTF8AndSize(r, &n);
    if (s == NULL) { sqlite3_result_error(ctx, "unicode encode error", -1); return; }
    sqlite3_result_text(ctx, s, (int)n, SQLITE_TRANSIENT);
    return;
  }
  if (PyBytes_Check(r)) {
    char *p = NULL; Py_ssize_t n = 0;
    if (PyBytes_AsStringAndSize(r, &p, &n) != 0) {
      sqlite3_result_error(ctx, "bytes decode error", -1); return;
    }
    sqlite3_result_blob(ctx, p, (int)n, SQLITE_TRANSIENT);
    return;
  }
  sqlite3_result_error(ctx, "invalid return type", -1);
  return;
}

void exec_python_inC(
	sqlite3_context *context,
	int argc,
	sqlite3_value **argv
	){
	PyObject *pFunc, *pArgs, *pValue, *pResult;
	int i;

	pFunc = (PyObject *)sqlite3_user_data(context);
	if (pFunc == NULL || !PyCallable_Check(pFunc)){
		sqlite3_result_error(context, "failed to call python function", -1);
		return;
		// pFuncの参照はdestroy_pyfuncにて減らすのでここではDECREFしない
	}

	pArgs = PyTuple_New(argc);
	if (pArgs == NULL) {
		sqlite3_result_error(context, "failed to create python tuple", -1);
		return;
	}

	for (i = 0; i < argc; ++i) {
		// 引数の型の判別
		int nType = sqlite3_value_type(argv[i]);
		switch (nType) {
		case SQLITE_INTEGER: {
			// 64bit 整数 → Python int
			sqlite3_int64 v = sqlite3_value_int64(argv[i]);
			pValue = PyLong_FromLongLong(v);
			break;
		}
		case SQLITE_FLOAT: {
			// 浮動小数 → Python float
			double v = sqlite3_value_double(argv[i]);
			pValue = PyFloat_FromDouble(v);
			break;
		}
		case SQLITE_TEXT: {
			// UTF-8 テキスト → Python str
			const unsigned char *t = sqlite3_value_text(argv[i]);
			int n = sqlite3_value_bytes(argv[i]);
			if (t != NULL) {
			pValue = PyUnicode_DecodeUTF8((const char*)t, n, "strict");
			}
			break;
		}
		case SQLITE_BLOB: {
			// BLOB → Python bytes
			const void *b = sqlite3_value_blob(argv[i]);
			int n = sqlite3_value_bytes(argv[i]);
			if (b != NULL) {
			pValue = PyBytes_FromStringAndSize((const char*)b, n);
			} else {
			pValue = PyBytes_FromStringAndSize("", 0);
			}
			break;
		}
		case SQLITE_NULL: {
			// NULL → None
			Py_INCREF(Py_None);
			pValue = Py_None;
			break;
		}
		default: {
			// 未対応型
			sqlite3_result_error(context, "invalid argumment type", -1);
			Py_DECREF(pArgs);
			return;
		}
		}
		if (!pValue) {
			Py_DECREF(pArgs);
			sqlite3_result_error(context, "failed to convert argument", -1);
			return;
		}
		// pArgsがpValueの参照を盗む仕様になっているため、pValueのDECREFは必要ない
		PyTuple_SetItem(pArgs, i, pValue);
	}

	// 実行
	pResult = PyObject_CallObject(pFunc, pArgs);
	Py_DECREF(pArgs);

	if (pResult != NULL) {
		if (pResult != NULL) {
		set_sqlite_result_from_py(context, pResult); //型ごとに結果の変換を行う関数
		Py_DECREF(pResult);
		} else {
		PyErr_Print();
		sqlite3_result_error(context, "failed to call python", -1);
		}
	} else {
		PyErr_Print();
		sqlite3_result_error(context, "failed to call python", -1);
	}
}

void destroy_pyfunc(void *pUserData){
	PyObject *pFunc = (PyObject *)pUserData;
	if (pFunc){
		Py_DECREF(pFunc);
	}
}

// sqlite3_create_function_v2周りのヘルパ関数

int register_pyfunc(sqlite3 *db, const char *zName, int nArg, PyObject *callable){
	if (!db || !zName || !callable || !PyCallable_Check(callable)) {
		return SQLITE_MISUSE;
	}

	Py_INCREF(callable);

	int rc = sqlite3_create_function_v2(
		db, // データベース
		zName, // 作成するSQL関数の名前
		-1, // 引数の個数（-1とすると任意）
		SQLITE_UTF8 | SQLITE_DETERMINISTIC, // テキストエンコーディング
		callable, // 実行用のC関数に渡すpythonオブジェクト
		exec_python_inC, // 実行用のC言語関数
		NULL, 
		NULL, 
		destroy_pyfunc // pFuncのデストラクタ
	);

	if (rc != SQLITE_OK){
		Py_DECREF(callable);
	}
	return rc;
}
