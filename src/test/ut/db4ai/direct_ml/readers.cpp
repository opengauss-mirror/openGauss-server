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
 * readers.cpp
 *        Read data files
 *
 *
 * IDENTIFICATION
 *        src/test/ut/db4ai/direct_ml/readers.cpp
 *
 * ---------------------------------------------------------------------------------------
**/

#include "readers.h"

extern ArrayType *construct_empty_md_array(uint32_t const num_centroids, uint32_t const dimension);

#define MaxColumnsCSV 2048

bool ReaderCSV::has_nulls() {
    return !no_nulls;
}

bool ReaderCSV::is_open() {
    return (allocated > 0);
}

void ReaderCSV::open() {
    Assert(!is_open());

	FILE* fp = fopen(filename, "rb");
	if (fp == nullptr)
		elog(ERROR, "invalid file '%s'", filename);

	fseek(fp, 0, SEEK_END);
	int end = ftell(fp);
	fseek(fp, 0, SEEK_SET);

	/*
	 * if a line is larger than 1 GB we're in trouble because we
	 * won't be able to allocate it in one chunk (not a problem for now)
	 * who has a single line of 1 GB anyway?
	 */
	const int MaxLineSize = 1000 * 1000 * 1000;
	char *buffer = (char*)palloc(MaxLineSize);

    nrows = 0;
	tuple.ncolumns = 0;

	char **tokens = (char**) palloc(sizeof(char*) * MaxColumnsCSV);

	if (header) {
		// read first line
		if (fgets(buffer, MaxLineSize, fp) != buffer)
			elog(ERROR, "cannot read CSV header");

		tuple.ncolumns = tokenize(buffer, tokens, delim);
		if (tuple.ncolumns == 0)
			elog(ERROR, "empty or wrong CSV header");
	}

	int remaining_types = 0;
	tuple.typid = nullptr;
	while (fgets(buffer, MaxLineSize, fp) == buffer) {
		// printf("ROW <%s>\n", buffer);
		int nc = tokenize(buffer, tokens, delim);

		// empty line, do not continue scanning
		if (nc == 0)
			break;

        if (nrows == 0) {
            // speculate with the number of rows
            double bc = ftell(fp);
            if (header)
                bc /= 2;

            allocated = (int)(end / bc + 1);
            // printf("allocate %d\n", allocated);
            rows = (uint8_t**)palloc(sizeof(uint8_t*) * allocated);
        } else if (nrows == allocated) {
            // resize
			int pos = ftell(fp);
			double ratio = pos / (double)nrows;
			allocated += (int)((end - pos) / ratio + 10);
            // printf("reallocate %d\n", allocated);
            rows = (uint8_t**)repalloc(rows, sizeof(uint8_t*) * allocated);
        }

		if (header) {
			if (nc != tuple.ncolumns)
				elog(ERROR, "expected %d columns in row %d, found %d", tuple.ncolumns, nrows, nc);
		} else {
			tuple.ncolumns = nc;
			header = false;
		}

		// prepare datatypes the first time
		if (tuple.typid == nullptr) {
			remaining_types = tuple.ncolumns;
			tuple.typid = (Oid*)palloc0(tuple.ncolumns * sizeof(Oid));
			tuple.typlen = (int16*)palloc0(tuple.ncolumns * sizeof(int16));
			tuple.typbyval = (bool*)palloc0(tuple.ncolumns * sizeof(bool));
		}

		// extract remaining datatypes
        char typalign;
		char *endp;
		if (remaining_types > 0) {
			for (int c=0 ; c<tuple.ncolumns ; c++) {
				if (tuple.typid[c] == InvalidOid) {
					char *str = tokens[c];
					if (*str != '\0') {
						strtol(str, &endp, 10);
						if (*endp == '\0')
							tuple.typid[c] = INT4OID;
						else {
							strtod(str, &endp);
							if (*endp == '\0')
								tuple.typid[c] = FLOAT8OID;
							else {
								if (strcasecmp(str, "true")==0 || strcasecmp(str, "false")==0)
									tuple.typid[c] = BOOLOID;
								else
									tuple.typid[c] = TEXTOID;
							}
						}
                        get_typlenbyvalalign(tuple.typid[c], &tuple.typlen[c], &tuple.typbyval[c], &typalign);
						remaining_types--;
					}
				}
			}
		}

		// read row
		int bc = tuple.ncolumns * (sizeof(bool) + sizeof(Datum));
        uint8_t *data = (uint8_t*)palloc(bc);
        rows[nrows++] = data;

		bool *isnull = (bool*)data;
		Datum *values = (Datum*)(data + tuple.ncolumns * sizeof(bool));

		for (int c=0 ; c<tuple.ncolumns ; c++) {
			char *str = tokens[c];
			if (*str != '\0') {
				isnull[c] = false;
				repeat:
				switch (tuple.typid[c]) {
					case BOOLOID:
						if (strcasecmp(str, "true")==0)
							values[c] = BoolGetDatum(true);
						else if (strcasecmp(str, "false")==0)
								values[c] = BoolGetDatum(true);
						else {
							promote(c, CSTRINGOID);
							goto repeat;
						}
						break;
					case INT4OID:
						values[c] = Int32GetDatum(strtol(str, &endp, 10));
						if (*endp != '\0') {
							// coerce
							values[c] = Float8GetDatum(strtod(str, &endp));
							if (*endp == '\0')
								promote(c, FLOAT8OID);
							else
								promote(c, CSTRINGOID);
							goto repeat;
						}
						break;
					case FLOAT8OID:
						values[c] = Float8GetDatum(strtod(str, &endp));
						if (*endp != '\0') {
							promote(c, CSTRINGOID);
							goto repeat;
						}
						break;
					case TEXTOID:
						values[c] = CStringGetTextDatum(str);
						break;
					case InvalidOid:
					default:
						Assert(false);
						break;
				}
			} else {
                if (no_nulls)
                    elog(ERROR, "found NULL in row %d, column %d when dataset was expected without NULLS", nrows, c+1);

				isnull[c] = true;
				values[c] = InvalidOid;
			}
		}
	}

	if (tuple.ncolumns == 0 || nrows == 0)
		elog(ERROR, "empty CSV dataset");

	pfree(tokens);
	pfree(buffer);
	fclose(fp);

	// start iterator
	next = 0;
}

void ReaderCSV::close() {
    Assert(is_open());

    pfree(tuple.typid);
    pfree(tuple.typlen);
    pfree(tuple.typbyval);
    pfree(rows);
    allocated = 0;

    pfree(filename);
    filename = nullptr;
}

Oid ReaderCSV::get_column_type(int column) {
    Assert(is_open());
	Assert(column < tuple.ncolumns);
	return tuple.typid[column];
}

int ReaderCSV::get_columns() {
    Assert(is_open());
    return tuple.ncolumns;
}

void ReaderCSV::rescan() {
    Assert(is_open());
    next = 0;
}

const ModelTuple *ReaderCSV::fetch() {
    Assert(is_open());

	if (next == nrows)
		return nullptr;

    uint8_t *data = rows[next++];
	tuple.isnull = (bool*)data;
	tuple.values = (Datum*)(data + tuple.ncolumns * sizeof(bool));

	return &tuple;
}

void ReaderCSV::promote(int column, Oid new_typid) {
	char buff[200];
	int curr = 0;
	while (curr < nrows-1) {
        uint8_t *data = rows[curr++];
		bool *isnull = (bool*)data;
		Datum *values = (Datum*)(data + tuple.ncolumns * sizeof(bool));
		if (!isnull[column]) {
			Datum dt = values[column];
			switch (tuple.typid[column]) {
				case BOOLOID:
					if (new_typid == TEXTOID)
						dt = CStringGetTextDatum(DatumGetBool(dt) ? "true" : "false");
					else
						elog(ERROR, "cannot promote from BOOL to %d", new_typid);
					break;
				case INT4OID:
					switch (new_typid) {
						case FLOAT8OID:
							dt = Float8GetDatum(DatumGetInt32(dt));
							break;
						case TEXTOID:
							sprintf(buff, "%d", DatumGetInt32(dt));
							dt = CStringGetTextDatum(buff);
							break;
						default:
							elog(ERROR, "cannot promote from INT to %d", new_typid);
					}
					break;
				case FLOAT8OID:
					if (new_typid == TEXTOID) {
						sprintf(buff, "%.16g", DatumGetFloat8(dt));
						dt = CStringGetTextDatum(buff);
					}
					else
						elog(ERROR, "cannot promote from FLOAT to %d", new_typid);
					break;
				default:
					elog(ERROR, "cannot promote from %d to %d", tuple.typid[column], new_typid);
			}
			values[column] = dt;
		}
	}

    char typalign;
    get_typlenbyvalalign(new_typid, &tuple.typlen[column], &tuple.typbyval[column], &typalign);
	tuple.typid[column] = new_typid;
}

int ReaderCSV::tokenize(char* str, char **tokens, char delim) {
	// remove end of line
	int len = strlen(str);
	while (len > 0) {
		len--;
		if (str[len]!='\n' && str[len]!='\r')
			break;
		str[len] = 0;
	}

    int ntokens = 0;
	if (len > 0) {
		bool has_delim;
		do {
			has_delim = false;
			
			// trim left
			while (*str != '\0') {
				if (*str!=' ' && *str!='\t')
					break;
				str++;
			}

			// printf("<%s>\n", str);

			bool quoted = (*str == '\"');
			if (quoted)
				str++;

			bool after_quoted = false;
			char *start = str;
			char *end = str;
			while (*str != 0) {
				char ch = *str++;
				if (after_quoted && ch != delim && ch != ' ' && ch != '\t')
					elog(ERROR, "invalid quoted string <%s>", start);

				if (ch == '\"') {
					if (*str == '\"') {
						if (!quoted)
							elog(ERROR, "invalid quoted string <%s>", start);

						str++; // ignore it
					} else {
						if (quoted) {
							after_quoted = true;
							quoted = false;
							continue;
						} else
							elog(ERROR, "invalid quoted string <%s>", start);
					}
				}

				if (ch == delim) {
					if (!quoted) {
						has_delim = true;
						break;
					}
				}

				*end++ = ch;
			}
			*end = 0;

			if (quoted)
				elog(ERROR, "invalid quoted string <%s>", start);

			// trim right
			while (end > start) {
				end--;
				if (*end!=' ' && *end!='\t')
					break;
				*end = 0;
			}

            if (ntokens == MaxColumnsCSV)
                elog(FATAL, "too many columns %d, maximum %d", ntokens, MaxColumnsCSV);

			// printf(" -- %d <%s>\n", ntokens, start);
			tokens[ntokens++] = start;
		} while(*str != 0);

		if (has_delim) {
			// null value at end
            if (ntokens == MaxColumnsCSV)
                elog(FATAL, "too many columns (adding last null)");

			tokens[ntokens++] = str;
		}
	}

	return ntokens;
}

void ReaderCSV::shuffle(int seed) {
    srand(seed);
    for (int r=0; r<nrows ; r++) {
        int d = rand() % nrows;
        uint8_t *row = rows[r];
        rows[r] = rows[d];
        rows[d] = row;
    }
}

///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
// ReaderVectorFloat

bool ReaderVectorFloat::has_nulls() {
    return false;
}

void ReaderVectorFloat::open() {
    Assert(!is_open());
    parent->open();

    int ncolumns = parent->get_columns();
    for (int c=0 ; c<ncolumns ; c++) {
        Oid typ = parent->get_column_type(c);
        if (typ != BOOLOID && typ != INT4OID && typ != FLOAT8OID)
            elog(ERROR, "cannot vectorize columns of type %d", typ);

        if (typ != FLOAT8OID)
            only_float8 = false;
    }

    arr = construct_empty_md_array(1, ncolumns);

    char typalign;
    typid = FLOAT8ARRAYOID;
    get_typlenbyvalalign(typid, &typlen, &typbyval, &typalign);
    isnull = false;
    value = PointerGetDatum(arr);

    tuple.ncolumns = 1;
    tuple.isnull = &isnull;
    tuple.values = &value;
    tuple.typid = &typid;
    tuple.typbyval = &typbyval;
    tuple.typlen = &typlen;
}

void ReaderVectorFloat::close() {
    Assert(is_open());
    pfree(arr);
    tuple.ncolumns = 0;

    parent->close();
}

bool ReaderVectorFloat::is_open() {
    return tuple.ncolumns > 0;
}

int ReaderVectorFloat::get_columns() {
    Assert(is_open());
    return 1;
}

Oid ReaderVectorFloat::get_column_type(int column) {
    Assert(is_open());
    Assert(column == 0);
    return FLOAT8ARRAYOID;
}

void ReaderVectorFloat::rescan() {
    Assert(is_open());
    parent->rescan();
}

const ModelTuple * ReaderVectorFloat::fetch() {
    Assert(is_open());
    const ModelTuple *rtuple = parent->fetch();
    if (rtuple == nullptr)
        return nullptr;

    double *ptr = (double*) ARR_DATA_PTR(arr);
    if (only_float8 && !parent->has_nulls()) {
        // just copy directly
        int bc = rtuple->ncolumns * sizeof(float8);
        errno_t rc = memcpy_s(ptr, bc, rtuple->values, bc);
        securec_check(rc, "\0", "\0");
    } else {
        for (int c=0 ;c<rtuple->ncolumns ; c++) {
            double value = 0;
            if (!rtuple->isnull[c]) {
                Datum dt = rtuple->values[c];
                Oid typid = rtuple->typid[c];
                // sorted by most frequent use
                if (typid == FLOAT8OID)
                    value = DatumGetFloat8(dt);
                else if (typid == INT4OID)
                        value = DatumGetInt32(dt);
                else {
                    Assert(typid == BOOLOID);
                    value = DatumGetBool(dt);
                }
                *ptr++ = value;
            }
        }
    }

    return &tuple;
}


///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////
// ReaderProjection

bool ReaderProjection::has_nulls() {
    return !no_nulls;
}

ReaderProjection::ReaderProjection(Reader *parent, Datum *projection)
: parent(parent) {
    // first count the size of the projection and the number of column
    tuple.ncolumns = 0;
    tuple.values = nullptr;

    Datum *proj = projection;
    while (*proj != 0) {
        // scan expression
        do {
            proj += 2; // atom
        } while (*proj != 0);
        proj++; // return
        tuple.ncolumns++;
    }
    proj++;
    Assert(tuple.ncolumns > 0);

    errno_t rc;
    int bc = (proj - projection) * sizeof(Datum);
    this->projection = (Datum*)palloc(bc);
    rc = memcpy_s(this->projection, bc, projection, bc);
    securec_check(rc, "\0", "\0");
}

bool ReaderProjection::is_open() {
    return tuple.values != nullptr;
}

void ReaderProjection::open() {
    Assert(!is_open());
    parent->open();

    // allocate tuple
    tuple.values = (Datum*)palloc(tuple.ncolumns * sizeof(Datum));
    tuple.isnull = (bool*)palloc(tuple.ncolumns * sizeof(bool));
    tuple.typid = (Oid*)palloc(tuple.ncolumns * sizeof(Oid));
    tuple.typlen = (int16*)palloc(tuple.ncolumns * sizeof(int16));
    tuple.typbyval = (bool*)palloc(tuple.ncolumns * sizeof(bool));

    // projection types are unknown, evaluate the 
    // projection with a fake row
    int ncols = parent->get_columns();
    bool isnull[ncols];
    Oid typid[ncols];
    Datum values[ncols];
    for (int c=0 ; c<ncols ; c++) {
        isnull[c] = false;
        typid[c] = parent->get_column_type(c);
        switch (typid[c]) {
            case INT4OID:
                values[c] = Int32GetDatum(1);
                break;
            case FLOAT8OID:
                values[c] = Float8GetDatum(1);
                break;
            case BOOLOID:
                values[c] = BoolGetDatum(true);
                break;
            case CSTRINGOID:
                values[c] = CStringGetDatum("fake");
                break;
            case TEXTOID:
                values[c] = CStringGetTextDatum("fake");
                break;
            default:
                elog(ERROR, "projection of type %d not supported", typid[c]);
                break;
        }
    }

    nlocators = 0;
    no_nulls = true;
    eval(values, isnull, typid, true);
}

void ReaderProjection::close() {
    Assert(is_open());

    pfree(tuple.values);
    pfree(tuple.isnull);
    pfree(tuple.typid);
    pfree(tuple.typlen);
    pfree(tuple.typbyval);
    tuple.values = nullptr;

    parent->close();
}

int ReaderProjection::get_columns() {
    Assert(is_open());
    return tuple.ncolumns;
}

Oid ReaderProjection::get_column_type(int column) {
    Assert(is_open());
    Assert(column < tuple.ncolumns);
    return tuple.typid[column];
}

void ReaderProjection::rescan() {
    Assert(is_open());
    parent->rescan();
}

const ModelTuple * ReaderProjection::fetch() {
    Assert(is_open());

    const ModelTuple *rtuple = parent->fetch();
    if (rtuple == nullptr)
        return nullptr;

    eval(rtuple->values, rtuple->isnull, rtuple->typid, false);
    return &tuple;
}

void ReaderProjection::eval(Datum* row, bool *isnull, Oid *typid, bool is_fake) {
    const int MaxLocators = sizeof(locators) / sizeof(locators[0]);
    const int MaxStackSize = 20;
    Datum stack_values[MaxStackSize];
    bool stack_isnull[MaxStackSize];
    Oid stack_typid[MaxStackSize];
    int col = 0;
    int index;
    PGFunction func;
    const FmgrBuiltin *fl;
    Datum *proj = projection;
    while (*proj != 0) {
        int sp = 0;
        bool has_func = false;
        // scan expression
        do {
            int op = DatumGetInt32(*proj++);
            switch (op) {
                case 1:
                    // call func
                    has_func = true;
                    fl = nullptr;
                    func = (PGFunction) DatumGetPointer(*proj++);
                    if (is_fake) {
                        // look for function into the catalog
                        if (nlocators == MaxLocators)
                            elog(ERROR, "too many func locators");

                        bool found = false;
                        fl = fmgr_builtins;
                        for (int b=0 ; !found && b<fmgr_nbuiltins ; b++) {
                            found = (fl->func == func);
                            if (found)
                                locators[nlocators++] = fl;
                            else
                                fl++;
                        }
                        Assert(found);
                    } else {
                        for (int b=0 ; b<nlocators ; b++) {
                            fl = locators[b];
                            if (fl->func == func)
                                break;
                        }
                    }

                    if (sp < fl->nargs)
                        elog(ERROR, "not enough operands (%d of %d) for function '%s'", sp, fl->nargs, fl->funcName);

                    switch (fl->nargs) {
                        case 1:
                            if (stack_isnull[sp-1])
                                elog(ERROR, "function parameters cannot be NULL");

                            stack_values[sp-1] = DirectFunctionCall1(func, stack_values[sp-1]);
                            break;
                        case 2:
                            // printf("CALL[%d] TYPE %d\n", sp, fl->rettype);

                            sp--;
                            if (stack_isnull[sp-1] || stack_isnull[sp])
                                elog(ERROR, "function parameters cannot be NULL");

                            stack_values[sp-1] = DirectFunctionCall2(func, stack_values[sp-1], stack_values[sp]);
                            break;
                        default:
                            elog(ERROR, "direct call with %d parameters not implemented", fl->nargs);
                            break;
                    }

                    stack_isnull[sp-1] = false;
                    stack_typid[sp-1] = fl->rettype;
                    break;
                case 2:
                    // push value of column from input row
                    if (sp == MaxStackSize)
                        elog(ERROR, "stack overflow in projection");

                    index = DatumGetInt32(*proj++);
                    if (index >= parent->get_columns())
                        elog(ERROR, "invalid input column %d (of %d) for projection", index, parent->get_columns());

                    // printf("PUSH[%d] COL %d TYPE %d\n", sp, index, typid[index]);

                    stack_values[sp] = row[index];
                    stack_isnull[sp] = isnull[index];
                    stack_typid[sp] = typid[index];
                    sp++;
                    break;
                case INT4OID:
                case TEXTOID:
                    // push int into stack
                    if (sp == MaxStackSize)
                        elog(ERROR, "stack overflow in projection");
                    stack_values[sp] = *proj++;
                    stack_isnull[sp] = false;
                    stack_typid[sp] = op;
                    sp++;
                    break;
                default:
                    elog(FATAL, "unkown operation %d", op);
            }
        } while (*proj != 0);
        proj++;

        // get result
        if (sp != 1)
            elog(ERROR, "invalid projection expression");

        if (is_fake) {
            // may return nulls?
            if (!has_func && no_nulls && parent->has_nulls())
                no_nulls = false;

            // just get the info
            if (stack_isnull[0])
                elog(ERROR, "cannot infer datatype of column %d in projection", col);

            char typalign;
            tuple.typid[col] = stack_typid[0];
            get_typlenbyvalalign(tuple.typid[col], &tuple.typlen[col], &tuple.typbyval[col], &typalign);
            // printf("COLUMN %d TYPE %d:%d:%d\n", col, tuple.typid[col], tuple.typlen[col], tuple.typbyval[col]);
        } else {
            tuple.isnull[col] = stack_isnull[0];
            if (!tuple.isnull[col]) {
                // printf("FETCH %d TYPE %d:%d\n", col, tuple.typid[col], stack_typid[0]);
                Assert(tuple.typid[col] == stack_typid[0]);
                tuple.values[col] = stack_values[0];
            }
        }

        col++;
    }
}


///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

void test_readers() {
    Datum proj[] = {
        // first
        DP_PROJECT_COLUMN(0),
        DP_PROJECT_INT4(5),
        DP_PROJECT_CALL(int4lt),
        DP_PROJECT_RETURN,
        // second
        DP_PROJECT_COLUMN(3),
        DP_PROJECT_RETURN,
        // done
        DP_PROJECT_RETURN,
    };
    (void)proj;

	ReaderCSV csv(
		// "/usr1/db4ai/opengauss/src/test/db4ai/data/patients.txt", false
		// "/usr1/db4ai/opengauss/src/test/db4ai/data/coordinates.txt", false
		// "/usr1/datasets/test.txt", true
		// "/usr1/datasets/perfect.txt", true
		"/usr1/datasets/houses.txt", false, true, '|'
		// "/usr1/datasets/s_cir.txt", false
		// "/usr1/datasets/virus.csv", false
        // with text
		// "/usr1/datasets/cities.txt", true
		// "/usr1/datasets/addresses.txt", false, false
		);
    Reader *rd = &csv;

	// ReaderVectorFloat vf(rd);
	// rd = &vf;

	// ReaderProjection vp(rd, proj);
	// rd = &vp;

	rd->open();

	printf("COLS=%d {", rd->get_columns());
	for (int c=0 ; c<rd->get_columns() ; c++)
		printf(" %s%d", c>0 ? "," : "", rd->get_column_type(c));
	printf(" }\n");

	int nrows = 0;
	while (rd->fetch() != nullptr)
		nrows++;

	csv.rescan();

	int nr = 0;
	const ModelTuple *tuple;
	while ((tuple = rd->fetch()) != nullptr) {
		if (nr < 10 || nr > nrows-10) {
			printf("#%d : {", nr+1);
			for (int c=0 ; c<tuple->ncolumns; c++) {
				if (c > 0)
					printf(", ");
				// printf("%d:", c+1);fflush(stdout);
				if (!tuple->isnull[c]) {
					if (tuple->typid[c] == CSTRINGOID)
						printf("\"");

					Oid typOutput;
					bool typIsVarlena;
					getTypeOutputInfo(tuple->typid[c], &typOutput, &typIsVarlena);
					char *str = OidOutputFunctionCall(typOutput, tuple->values[c]);
					printf("%s",str);
					pfree(str);

					if (tuple->typid[c] == CSTRINGOID)
						printf("\"");
				}
			}
			printf("}\n");
		}
		nr++;
	}
	Assert(nr == nrows);

	rd->close();
}


