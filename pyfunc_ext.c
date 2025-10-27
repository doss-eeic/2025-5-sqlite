#include "pyudf.h"

/* --- 元の実装に合わせた実行関数 --- */
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
		if (nType == SQLITE_INTEGER){
		// sqlite3の整数からpythonの整数へと変換
		pValue = PyLong_FromLongLong(sqlite3_value_int64(argv[i]));
		if (!pValue) {
			Py_DECREF(pArgs);
			sqlite3_result_error(context, "failed to convert argument to python int", -1);
			return;
		}
		} else {
		 //*** 今のところ整数型以外は以下の処理によってエラーにしている。floatやstringへの拡張の余地あり。 ***
		Py_DECREF(pArgs);
		sqlite3_result_error(context, "invalid argument type", -1);
		return;
		}

		// pArgsがpValueの参照を盗む仕様になっているため、pValueのDECREFは必要ない
		PyTuple_SetItem(pArgs, i, pValue);
	}

	// 実行
	pResult = PyObject_CallObject(pFunc, pArgs);
	Py_DECREF(pArgs);

	if (pResult != NULL) {
		//　pResultの型に応じてsqlite3からpythonへと型変換
		if (PyLong_Check(pResult)) {
		sqlite3_result_int64(context, PyLong_AsLongLong(pResult));
		} else {
		sqlite3_result_error(context, "invalid argument type", -1);
		}
		Py_DECREF(pResult);
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
		SqlFunc, // 作成するSQL関数の名前
		-1, // 引数の個数（-1とすると任意）
		SQLITE_UTF8 | SQLITE_DETERMINISTIC, // テキストエンコーディング
		pFunc, // 実行用のC関数に渡すpythonオブジェクト
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
