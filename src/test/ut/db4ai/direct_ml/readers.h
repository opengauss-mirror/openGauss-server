/**
Copyright (c) 2021 Huawei Technologies Co.,Ltd. 
openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

  http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
---------------------------------------------------------------------------------------
 *
 * readers.h
 *        Header file of reading data files
 *
 *
 * IDENTIFICATION
 *        src/test/ut/db4ai/direct_ml/readers.h
 *
 * ---------------------------------------------------------------------------------------
**/


#ifndef MOCK_READERS_H
#define MOCK_READERS_H

#include "postgres.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/fmgrtab.h"
#include "db4ai/kernel.h"

/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

class Reader {
public:
	virtual ~Reader() {}

	virtual bool has_nulls() = 0;

    virtual void open() = 0;

	virtual bool is_open() = 0;

    virtual void close() = 0;

	virtual int get_columns() = 0;

	virtual Oid get_column_type(int column) = 0;

	virtual const ModelTuple *fetch() = 0;

	virtual void rescan() = 0;

protected:
    Reader() {} // to force inheritance
};

class ReaderCSV : public Reader {
public:
    ReaderCSV(const char* filename, bool header, bool no_nulls = true, char delim = ',')
    : header(header)
    , delim(delim)
	, no_nulls(no_nulls)
    , allocated(0) {
        this->filename = pstrdup(filename);
    }

	virtual bool has_nulls() override;

	virtual void open() override;

	virtual bool is_open() override;

    virtual void close() override;

	virtual int get_columns() override;

	virtual Oid get_column_type(int column) override;

	virtual const ModelTuple *fetch() override;

	virtual void rescan() override;

	void shuffle(int seed);

private:
    char* filename;
    bool header;
    char delim;
	bool no_nulls;
	int allocated;
	int nrows;
	uint8_t **rows; // nulls + Datums
	int next;
	ModelTuple tuple;

	int tokenize(char* str, char ** tokens, char delim);
	void promote(int column, Oid new_typid);
};

/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

class ReaderVectorFloat : public Reader {
public:
	ReaderVectorFloat(Reader *parent)
    : parent(parent)
    , arr(nullptr)
	, only_float8(true) {
        tuple.ncolumns = 0;
    }

	virtual bool has_nulls() override;

	virtual void open() override;

	virtual bool is_open() override;

    virtual void close() override;

	virtual int get_columns() override;

	virtual Oid get_column_type(int column) override;

	virtual const ModelTuple *fetch() override;

	virtual void rescan() override;

private:
	Reader *parent;
	ModelTuple tuple;
    Oid typid;
    int16 typlen;
    bool typbyval;
    bool isnull;
    Datum value;
    ArrayType *arr;
	bool only_float8;
};

/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

// A projection expression is a postorder sequence of operations (INT4, COLUMN, CALL, ...)
// finished by a RETURN. Functions can be any defined into builtins.h. Evaluation is with
// a stack machine. For example, to multiple column 3 by 2 and to return column 1:
//
// -- first column 
// DP_PROJECT_COLUMN(3)
// DP_PROJECT_INT4(2)
// DP_PROJECT_CALL(int4mul)
// DP_PROJECT_RETURN
// -- second column 
// DP_PROJECT_COLUMN(1)
// DP_PROJECT_RETURN
// -- done 
// DP_PROJECT_RETURN
// 
// A projection is a sequence of projection expression
// followed by an extra final RETURN (empty expresion)

// Utility macros to define projections. Notice that each atom of the projection
// is defined by a pair of Datums except the end (RETURN) which is a single one,
// and that the first element of the pair is always an INT4OID
#define DP_PROJECT_RETURN           0
#define DP_PROJECT_CALL(_FUNC)      Int32GetDatum(1), PointerGetDatum(_FUNC)
#define DP_PROJECT_COLUMN(_COL)     Int32GetDatum(2), Int32GetDatum(_COL)
#define DP_PROJECT_INT4(_VAL)       Int32GetDatum(INT4OID), Int32GetDatum(_VAL)
#define DP_PROJECT_TEXT(_VAL)    	Int32GetDatum(TEXTOID), CStringGetTextDatum(_VAL)

class ReaderProjection : public Reader {
public:
	ReaderProjection(Reader *parent, Datum *projection);

	virtual bool has_nulls() override;

	virtual void open() override;

	virtual bool is_open() override;

    virtual void close() override;

	virtual int get_columns() override;

	virtual Oid get_column_type(int column) override;

	virtual const ModelTuple *fetch() override;

	virtual void rescan() override;

private:
	Reader *parent;
	ModelTuple tuple;
    Datum *projection;
    const FmgrBuiltin *locators[10];
    int nlocators;
	bool no_nulls;

    void eval(Datum* row, bool *isnull, Oid *typid, bool is_fake);
};

////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////

template<class _K>
class ReaderKernel : public Reader {
public:
	ReaderKernel(Reader *parent)
    : parent(parent) {
    }

	bool has_nulls() override {
        Assert(parent->is_open());
        return parent->has_nulls();
    }

	void open() override {
        Assert(!parent->is_open());
        parent->open();
        for (int c=1 ; c<parent->get_columns() ; c++) {
            Assert(parent->get_column_type(c) == FLOAT8OID);
        }
        init();
    }

	bool is_open() override {
        return parent->is_open();
    }

    void close() override {
        Assert(parent->is_open());
        release();
        parent->close();
    }

	int get_columns() override {
        Assert(parent->is_open());
        return tuple.ncolumns;
    }

	Oid get_column_type(int column) override {
        Assert(parent->is_open());
        if (column == 0)
            return parent->get_column_type(0);
        return FLOAT8OID;
    }

	const ModelTuple *fetch() override {
        Assert(parent->is_open());
        const ModelTuple *ptuple = parent->fetch();
        if (ptuple != NULL && !ptuple->isnull[0]) {
            tuple.values[0] = ptuple->values[0];
            tuple.isnull[0] = ptuple->isnull[0];
            tuple.typid[0] = ptuple->typid[0];
            tuple.typbyval[0] = ptuple->typbyval[0];
            tuple.typlen[0] = ptuple->typlen[0];

            Datum *pvalues = &ptuple->values[1];
            float8 *pdata = tmp.data;
            for (int c=1 ; c<=tmp.rows ; c++) {
                Assert(!ptuple->isnull[c]);
                Assert(ptuple->typid[c] == FLOAT8OID);
                *pdata++ = DatumGetFloat8(*pvalues++);
            }

            kernel.km.transform(&kernel.km, &tmp, &out);

            pdata = out.data;
            pvalues = &tuple.values[1];
            for (int c=0 ; c<out.rows ; c++)
                *pvalues++ = Float8GetDatum(*pdata++);

            ptuple = &tuple;
        }
        return ptuple;
    }

	void rescan() override {
        Assert(parent->is_open());
        parent->rescan();
        // gamma may have changed
        release();
        init();
    }

protected:
    _K kernel;
    Matrix out;

private:
	Reader *parent;
    ModelTuple tuple;
    Matrix tmp;

    virtual void init_kernel(int features, int components, int seed) = 0;

    void init() {
        int features = parent->get_columns() - 1;
        int seed = 1;
        double D = Max(128, 2.0 * features);

        init_kernel(features, D, seed);
        tuple.ncolumns = kernel.km.coefficients + 1;

        tuple.isnull = (bool*)palloc(sizeof(bool) * tuple.ncolumns);
        tuple.values = (Datum*)palloc(sizeof(Datum) * tuple.ncolumns);
        tuple.typid = (Oid*)palloc(sizeof(Oid) * tuple.ncolumns);
        tuple.typbyval = (bool*)palloc(sizeof(bool) * tuple.ncolumns);
        tuple.typlen = (int16*)palloc(sizeof(int16) * tuple.ncolumns);

        for (int c=1 ; c<tuple.ncolumns ; c++) {
            tuple.isnull[c] = false;
            tuple.typid[c] = FLOAT8OID;
            tuple.typbyval[c] = true;
            tuple.typlen[c] = 8;
        }

        matrix_init(&tmp, features);
        matrix_init(&out, D);
    }

    void release() {
        kernel.km.release(&kernel.km);
        matrix_release(&tmp);
        matrix_release(&out);
    }
};

class ReaderGaussian : public ReaderKernel<KernelGaussian> {
public:
	ReaderGaussian(Reader *parent, double *gamma)
    : ReaderKernel(parent)
    , gamma(gamma) {
    }

private:
    double *gamma;

    void init_kernel(int features, int components, int seed) {
        kernel_init_gaussian(&kernel, features, components, *gamma, seed);
    }
};

class ReaderPolynomial : public ReaderKernel<KernelPolynomial> {
public:
	ReaderPolynomial(Reader *parent, int *degree, double *coef0)
    : ReaderKernel(parent)
    , degree(degree)
    , coef0(coef0) {
    }

private:
    int *degree;
    double *coef0;

    void init_kernel(int features, int components, int seed) {
        kernel_init_polynomial(&kernel, features, components, *degree, *coef0, seed);
    }
};

///////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////

void test_readers();

#endif


